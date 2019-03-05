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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Privilege.PPrivilege;
import alluxio.proto.journal.Privilege.PrivilegeUpdateEntry;
import alluxio.wire.Privilege;

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link PrivilegeMaster}.
 */
public final class PrivilegeMasterTest {
  private static PrivilegeMaster sMaster;
  private static MasterRegistry sRegistry;
  private static JournalSystem sJournalSystem;

  /** Rule to create a new temporary folder during each test. */
  @ClassRule
  public static TemporaryFolder sTestFolder = new TemporaryFolder();

  @BeforeClass
  public static void before() throws Exception {
    // To make sure Raft cluster and connect address match.
    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    sRegistry = new MasterRegistry();
    sJournalSystem = JournalTestUtils.createJournalSystem(sTestFolder);
    sMaster = new PrivilegeMasterFactory().create(sRegistry,
        MasterTestUtils.testMasterContext(sJournalSystem));

    sJournalSystem.start();
    sJournalSystem.gainPrimacy();
    sRegistry.start(true);
  }

  @AfterClass
  public static void after() throws Exception {
    sJournalSystem.stop();
    sRegistry.stop();
  }

  @Test
  public void processGrantAndRevokeJournalEntry() throws Exception {
    sMaster.processJournalEntry(JournalEntry.newBuilder().setPrivilegeUpdate(
        PrivilegeUpdateEntry.newBuilder()
        .setGroup("testGroup")
        .setGrant(true)
        .addAllPrivilege(Arrays.asList(PPrivilege.FREE_PRIVILEGE)))
        .build());
    assertTrue(sMaster.hasPrivilege("testGroup", Privilege.FREE));
    sMaster.processJournalEntry(JournalEntry.newBuilder().setPrivilegeUpdate(
        PrivilegeUpdateEntry.newBuilder()
        .setGroup("testGroup")
        .setGrant(false)
        .addAllPrivilege(Arrays.asList(PPrivilege.FREE_PRIVILEGE)))
        .build());
    assertFalse(sMaster.hasPrivilege("testGroup", Privilege.FREE));
  }

  @Test
  public void hasPrivilege() throws Exception {
    sMaster.updatePrivileges("testGroup", Arrays.asList(Privilege.FREE, Privilege.TTL), true);
    assertTrue(sMaster.hasPrivilege("testGroup", Privilege.FREE));
    assertTrue(sMaster.hasPrivilege("testGroup", Privilege.TTL));
    assertFalse(sMaster.hasPrivilege("testGroup", Privilege.REPLICATION));
    assertFalse(sMaster.hasPrivilege("testGroup", Privilege.PIN));
  }

  @Test
  public void otherGroupHasNoPrivilege() throws Exception {
    for (Privilege p : Privilege.values()) {
      assertFalse(sMaster.hasPrivilege("nonexist", p));
    }
  }

  @Test
  public void getPrivileges() throws Exception {
    List<Privilege> privileges = Arrays.asList(Privilege.FREE, Privilege.TTL);
    sMaster.updatePrivileges("testGroup", privileges, true);
    assertEquals(new HashSet<>(privileges), sMaster.getPrivileges("testGroup"));
  }

  @Test
  public void getGroupToPrivilegesMapping() throws Exception {
    sMaster.resetState();
    sMaster.updatePrivileges("group1", Arrays.asList(Privilege.FREE), true);
    sMaster.updatePrivileges("group2", Arrays.asList(Privilege.TTL, Privilege.FREE), true);
    Map<String, Set<Privilege>> expected = ImmutableMap.<String, Set<Privilege>>of(
        "group1", new HashSet<>(Arrays.asList(Privilege.FREE)),
        "group2", new HashSet<>(Arrays.asList(Privilege.TTL, Privilege.FREE)));
    assertEquals(expected, sMaster.getGroupToPrivilegesMapping());
  }
}
