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

package alluxio.cli.privileges;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Integration tests for {@ListPrivilegesCommand}.
 */
public final class ListPrivilegesCommandTest extends PrivilegesTest {
  @Test
  public void listExistingGroup() throws Exception {
    grantPrivileges("A", FREE, TTL);
    assertEquals("A: [FREE, TTL]\n", run("list", "-group", "A"));
  }

  @Test
  public void listNonexistingGroup() throws Exception {
    assertEquals("A: []\n", run("list", "-group", "A"));
  }

  @Test
  public void listTwoGroups() throws Exception {
    grantPrivileges("A");
    grantPrivileges("B", FREE, TTL);
    assertEquals("A: []\nB: [FREE, TTL]\n", run("list"));
  }

  @Test
  public void listGroupsAlphabetically() throws Exception {
    grantPrivileges("C");
    grantPrivileges("B");
    grantPrivileges("A");
    assertEquals("A: []\nB: []\nC: []\n", run("list"));
  }

  @Test
  public void listPrivilegesAlphabetically() throws Exception {
    grantPrivileges("A", PIN, TTL, FREE, REPLICATION);
    assertEquals("A: [FREE, PIN, REPLICATION, TTL]\n", run("list"));
  }

  @Test
  public void listUser() throws Exception {
    // Bob's only group is "nonsuper", this is set in PrivilegesTest.
    grantPrivileges("nonsuper", FREE, PIN);
    assertEquals("Bob: [FREE, PIN]\n", run("list", "-user", "Bob"));
  }

  @Test
  public void listUserAndGroup() throws Exception {
    assertTrue(run("list", "-user", "user", "-group", "group")
        .contains("Cannot specify both user and group"));
  }
}
