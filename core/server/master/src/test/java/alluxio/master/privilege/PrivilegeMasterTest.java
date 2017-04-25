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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.master.MasterRegistry;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Privilege.PPrivilege;
import alluxio.proto.journal.Privilege.PrivilegeUpdateEntry;
import alluxio.wire.Privilege;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link DefaultPrivilegeMaster}.
 */
public final class PrivilegeMasterTest {
  private DefaultPrivilegeMaster mMaster;
  private MasterRegistry mRegistry;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mRegistry = new MasterRegistry();
    JournalFactory factory =
        new Journal.Factory(new URI(mTestFolder.newFolder().getAbsolutePath()));
    mMaster = new DefaultPrivilegeMaster(factory);
    mRegistry.add(PrivilegeMaster.class, mMaster);
    mRegistry.start(true);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  @Test
  public void processGrantAndRevokeJournalEntry() throws Exception {
    mMaster.processJournalEntry(JournalEntry.newBuilder().setPrivilegeUpdate(
        PrivilegeUpdateEntry.newBuilder()
        .setGroup("testGroup")
        .setGrant(true)
        .addAllPrivilege(Arrays.asList(PPrivilege.FREE_PRIVILEGE)))
        .build());
    assertTrue(mMaster.hasPrivilege("testGroup", Privilege.FREE));
    mMaster.processJournalEntry(JournalEntry.newBuilder().setPrivilegeUpdate(
        PrivilegeUpdateEntry.newBuilder()
        .setGroup("testGroup")
        .setGrant(false)
        .addAllPrivilege(Arrays.asList(PPrivilege.FREE_PRIVILEGE)))
        .build());
    assertFalse(mMaster.hasPrivilege("testGroup", Privilege.FREE));
  }

  @Test
  public void hasPrivilege() throws Exception {
    mMaster.updatePrivileges("testGroup", Arrays.asList(Privilege.FREE, Privilege.TTL), true);
    assertTrue(mMaster.hasPrivilege("testGroup", Privilege.FREE));
    assertTrue(mMaster.hasPrivilege("testGroup", Privilege.TTL));
    assertFalse(mMaster.hasPrivilege("testGroup", Privilege.REPLICATION));
    assertFalse(mMaster.hasPrivilege("testGroup", Privilege.PIN));
  }

  @Test
  public void otherGroupHasNoPrivilege() throws Exception {
    for (Privilege p : Privilege.values()) {
      assertFalse(mMaster.hasPrivilege("nonexist", p));
    }
  }

  @Test
  public void getPrivileges() throws Exception {
    List<Privilege> privileges = Arrays.asList(Privilege.FREE, Privilege.TTL);
    mMaster.updatePrivileges("testGroup", privileges, true);
    assertEquals(new HashSet<>(privileges), mMaster.getPrivileges("testGroup"));
  }

  @Test
  public void getGroupToPrivilegesMapping() {
    mMaster.updatePrivileges("group1", Arrays.asList(Privilege.FREE), true);
    mMaster.updatePrivileges("group2", Arrays.asList(Privilege.TTL, Privilege.FREE), true);
    Map<String, Set<Privilege>> expected = ImmutableMap.<String, Set<Privilege>>of(
        "group1", new HashSet<>(Arrays.asList(Privilege.FREE)),
        "group2", new HashSet<>(Arrays.asList(Privilege.TTL, Privilege.FREE)));
    assertEquals(expected, mMaster.getGroupToPrivilegesMapping());
  }
}
