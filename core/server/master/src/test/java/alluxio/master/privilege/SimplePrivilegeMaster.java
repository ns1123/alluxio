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

import alluxio.clock.SystemClock;
import alluxio.master.AbstractMaster;
import alluxio.master.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.wire.Privilege;

import org.apache.thrift.TProcessor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A privilege checker controlled by a privileges map defined at construction.
 */
public final class SimplePrivilegeMaster extends AbstractMaster implements PrivilegeMaster {
  private final Map<String, Set<Privilege>> mGroupToPrivilegeMap;

  SimplePrivilegeMaster(Map<String, Set<Privilege>> groupToPrivilegeMap) {
    super(Mockito.mock(Journal.class), new SystemClock(),
        Mockito.mock(ExecutorServiceFactory.class));
    mGroupToPrivilegeMap = groupToPrivilegeMap;
  }

  @Override
  public Map<String, Set<Privilege>> getGroupToPrivilegesMapping() {
    return mGroupToPrivilegeMap;
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    // No Journal
    return CommonUtils.nullIterator();
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public Set<Privilege> getPrivileges(String group) {
    Set<Privilege> privileges = mGroupToPrivilegeMap.get(group);
    return privileges == null ? new HashSet<Privilege>() : privileges;
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return null;
  }

  @Override
  public boolean hasPrivilege(String group, Privilege privilege) {
    Set<Privilege> privileges = mGroupToPrivilegeMap.get(group);
    return privileges != null && privileges.contains(privilege);
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // No journal.
  }

  @Override
  public void start(Boolean unused) throws IOException {}

  @Override
  public void stop() throws IOException {}

  @Override
  public Set<Privilege> updatePrivileges(String group, List<Privilege> privileges, boolean grant) {
    return getPrivileges(group);
  }
}
