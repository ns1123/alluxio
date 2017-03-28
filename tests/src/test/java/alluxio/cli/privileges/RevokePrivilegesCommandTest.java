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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * Integration tests for {@RevokePrivilegesCommand}.
 */
public final class RevokePrivilegesCommandTest extends PrivilegesTest {
  @Test
  public void revokeSinglePrivilege() throws Exception {
    grantPrivileges("A", FREE, TTL);
    assertEquals("A: [TTL]\n", run("revoke", "-group", "A", "-privileges", "FREE"));
  }

  @Test
  public void revokeMissingPrivilege() throws Exception {
    assertEquals("A: []\n", run("revoke", "-group", "A", "-privileges", "FREE"));
  }

  @Test
  public void revokeSinglePrivilegeDifferentArgOrder() throws Exception {
    grantPrivileges("A", FREE, TTL);
    assertEquals("A: [TTL]\n", run("revoke", "-privileges", "FREE", "-group", "A"));
  }

  @Test
  public void revokeMultiplePrivileges() throws Exception {
    grantPrivileges("A", FREE, TTL, REPLICATION, PIN);
    assertEquals("A: [FREE, TTL]\n",
        run("revoke", "-group", "A", "-privileges", "REPLICATION", "PIN"));
  }

  @Test
  public void revokeMultiplePrivilegeArgs() throws Exception {
    grantPrivileges("A", FREE, TTL, REPLICATION, PIN);
    assertEquals("A: [FREE, TTL]\n",
        run("revoke", "-group", "A", "-privileges", "REPLICATION", "-privileges", "PIN"));
  }

  @Test
  public void caseInsensitive() throws Exception {
    grantPrivileges("A", FREE, TTL);
    assertEquals("A: [FREE]\n", run("revoke", "-group", "A", "-privileges", "tTl"));
  }

  @Test
  public void revokeAllPrivileges() throws Exception {
    grantPrivileges("A", FREE, TTL, REPLICATION, PIN);
    assertEquals("A: []\n",
        run("revoke", "-group", "A", "-privileges", "all"));
  }

  @Test
  public void revokeNoPrivileges() throws Exception {
    assertThat(run("revoke", "-group", "A", "-privileges", "none"),
        containsString("Cannot revoke NONE privilege"));
  }

  @Test
  public void revokeAllAndMore() throws Exception {
    assertThat(run("revoke", "-group", "A", "-privileges", "ALL", "FREE"),
        containsString("The ALL privilege should be specified on its own"));
  }

  @Test
  public void revokeNonexistentPrivilege() throws Exception {
    assertThat(run("revoke", "-group", "A", "-privileges", "notAPrivilege"),
        containsString("Unrecognized privilege: notAPrivilege"));
  }
}
