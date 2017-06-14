/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.privilege;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.clock.SystemClock;
import alluxio.exception.ExceptionMessage;
import alluxio.master.AbstractMaster;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalFactory;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Privilege.PrivilegeUpdateEntry;
import alluxio.resource.LockResource;
import alluxio.thrift.PrivilegeMasterClientService;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.Privilege;

import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A master for handling group to privilege mapping.
 */
@ThreadSafe
public final class DefaultPrivilegeMaster extends AbstractMaster implements PrivilegeMaster {
  private final Lock mGroupPrivilegesLock;

  /**
   * Mapping from group to privileges. Modifications to this field must be journaled.
   */
  @GuardedBy("mGroupPrivilegesLock")
  private final Map<String, Set<Privilege>> mGroupPrivileges;
  private final String mSupergroup;

  /**
   * Creates a new instance of {@link DefaultPrivilegeMaster}.
   *
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  DefaultPrivilegeMaster(JournalFactory journalFactory) {
    super(journalFactory.create(Constants.PRIVILEGE_MASTER_NAME), new SystemClock(),
        ExecutorServiceFactories
            .fixedThreadPoolExecutorServiceFactory(Constants.PRIVILEGE_MASTER_NAME, 1));
    mGroupPrivilegesLock = new ReentrantLock();
    mGroupPrivileges = new ConcurrentHashMap<>();
    mSupergroup = Configuration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.PRIVILEGE_MASTER_CLIENT_SERVICE_NAME,
        new PrivilegeMasterClientService.Processor<>(new PrivilegeMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.PRIVILEGE_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(alluxio.proto.journal.Journal.JournalEntry entry)
      throws IOException {
    if (entry.hasPrivilegeUpdate()) {
      PrivilegeUpdateEntry update = entry.getPrivilegeUpdate();
      updatePrivilegesInternal(update.getGroup(), update.getGrant(),
          PrivilegeUtils.fromProto(update.getPrivilegeList()));
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    final Iterator<Entry<String, Set<Privilege>>> it;
    // When iterating on the journal entries, the caller needs to guarantee that no one
    // is modifying the mGroupPrivileges. Lock is acquired here to ensure that all the changes
    // to mGroupPrivileges so far happens before the iteration.
    try (LockResource r = new LockResource(mGroupPrivilegesLock)) {
      it = mGroupPrivileges.entrySet().iterator();
    }
    return new Iterator<JournalEntry>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public JournalEntry next() {
        Entry<String, Set<Privilege>> entry = it.next();
        String group = entry.getKey();
        Set<Privilege> privileges = entry.getValue();
        return JournalEntry.newBuilder().setPrivilegeUpdate(
            PrivilegeUpdateEntry.newBuilder().setGroup(group).setGrant(true)
                .addAllPrivilege(PrivilegeUtils.toProto(privileges))).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "Privilegemaster#getJournalEntryIterator#remove is not supported.");
      }
    };
  }

  @Override
  public boolean hasPrivilege(String group, Privilege privilege) {
    Set<Privilege> privileges;
    try (LockResource r = new LockResource(mGroupPrivilegesLock)) {
      privileges = mGroupPrivileges.get(group);
    }
    return privileges != null && privileges.contains(privilege);
  }

  @Override
  public Set<Privilege> getPrivileges(String group) {
    if (group.equals(mSupergroup)) {
      return new HashSet<>(Arrays.asList(Privilege.values()));
    }
    try (LockResource r = new LockResource(mGroupPrivilegesLock)) {
      Set<Privilege> privileges = mGroupPrivileges.get(group);
      return privileges == null ? new HashSet<Privilege>() : privileges;
    }
  }

  @Override
  public Map<String, Set<Privilege>> getGroupToPrivilegesMapping() {
    Map<String, Set<Privilege>> privilegesMap = new HashMap<>();
    try (LockResource r = new LockResource(mGroupPrivilegesLock)) {
      for (Entry<String, Set<Privilege>> entry : mGroupPrivileges.entrySet()) {
        privilegesMap.put(entry.getKey(), new HashSet<>(entry.getValue()));
      }
    }
    return privilegesMap;
  }

  @Override
  public Set<Privilege> updatePrivileges(String group, List<Privilege> privileges, boolean grant) {
    try (JournalContext journalContext = createJournalContext();
        LockResource r = new LockResource(mGroupPrivilegesLock)) {
      updatePrivilegesInternal(group, grant, privileges);
      appendJournalEntry(JournalEntry.newBuilder().setPrivilegeUpdate(
          PrivilegeUpdateEntry.newBuilder()
          .setGroup(group)
          .setGrant(grant)
          .addAllPrivilege(PrivilegeUtils.toProto(privileges)))
          .build(), journalContext);
      return getPrivileges(group);
    }
  }

  /**
   * Grants or revokes privileges for a group.
   *
   * @param group the group to modify
   * @param grant if true, grant the specified privileges; otherwise revoke them
   * @param privileges the privileges to grant or revoke
   */
  private void updatePrivilegesInternal(String group, boolean grant, List<Privilege> privileges) {
    try (LockResource r = new LockResource(mGroupPrivilegesLock)) {
      Set<Privilege> groupPrivileges = mGroupPrivileges.get(group);
      if (groupPrivileges == null) {
        groupPrivileges = new HashSet<>();
        mGroupPrivileges.put(group, groupPrivileges);
      }
      if (grant) {
        groupPrivileges.addAll(privileges);
      } else {
        groupPrivileges.removeAll(privileges);
      }
    }
  }
}
