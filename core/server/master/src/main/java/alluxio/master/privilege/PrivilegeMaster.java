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

import alluxio.Constants;
import alluxio.clock.SystemClock;
import alluxio.exception.ExceptionMessage;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterRegistry;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Privilege.PrivilegeUpdateEntry;
import alluxio.resource.LockResource;
import alluxio.thrift.PrivilegeMasterClientService;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.Privilege;

import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
public final class PrivilegeMaster extends AbstractMaster implements PrivilegeService {
  private final Lock mGroupPrivilegesLock;

  /**
   * Mapping from group to privileges. Modifications to this field must be journaled.
   */
  @GuardedBy("mGroupPrivilegesLock")
  private final Map<String, Set<Privilege>> mGroupPrivileges;

  /**
   * Creates a new instance of {@link PrivilegeMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  public PrivilegeMaster(MasterRegistry registry, JournalFactory journalFactory) {
    super(journalFactory.create(Constants.PRIVILEGE_MASTER_NAME), new SystemClock(),
        ExecutorServiceFactories
            .fixedThreadPoolExecutorServiceFactory(Constants.PRIVILEGE_MASTER_NAME, 1));
    mGroupPrivilegesLock = new ReentrantLock();
    mGroupPrivileges = new ConcurrentHashMap<>();
    registry.add(PrivilegeMaster.class, this);
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
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream)
      throws IOException {
    try (LockResource r = new LockResource(mGroupPrivilegesLock)) {
      for (Entry<String, Set<Privilege>> entry : mGroupPrivileges.entrySet()) {
        String group = entry.getKey();
        Set<Privilege> privileges = entry.getValue();
        outputStream.write(JournalEntry.newBuilder()
            .setPrivilegeUpdate(PrivilegeUpdateEntry.newBuilder()
                .setGroup(group)
                .setGrant(true)
                .addAllPrivilege(PrivilegeUtils.toProto(privileges))).build());
      }
    }
  }

  @Override
  public boolean hasPrivilege(String group, Privilege privilege) {
    Set<Privilege> privileges;
    try (LockResource r = new LockResource(mGroupPrivilegesLock)) {
      privileges = mGroupPrivileges.get(group);
    }
    return privileges != null && privileges.contains(privilege);
  }

  /**
   * @param group the group to fetch the privileges for
   * @return the privileges for the group
   */
  public Set<Privilege> getPrivileges(String group) {
    try (LockResource r = new LockResource(mGroupPrivilegesLock)) {
      Set<Privilege> privileges = mGroupPrivileges.get(group);
      return privileges == null ? new HashSet<Privilege>() : privileges;
    }
  }

  /**
   * @return a snapshot of all group privilege information
   */
  public Map<String, Set<Privilege>> getGroupToPrivilegesMapping() {
    Map<String, Set<Privilege>> privilegesMap = new HashMap<>();
    try (LockResource r = new LockResource(mGroupPrivilegesLock)) {
      for (Entry<String, Set<Privilege>> entry : mGroupPrivileges.entrySet()) {
        privilegesMap.put(entry.getKey(), new HashSet<>(entry.getValue()));
      }
    }
    return privilegesMap;
  }

  /**
   * Updates privileges and journals the update.
   *
   * @param group the group to grant or revoke the privileges for
   * @param privileges the privileges to grant or revoke
   * @param grant if true, grant the privileges; otherwise revoke them
   * @return the updated privileges for the group
   */
  public Set<Privilege> updatePrivileges(String group, List<Privilege> privileges,
      boolean grant) {
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
