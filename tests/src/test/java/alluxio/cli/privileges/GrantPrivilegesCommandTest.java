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
 * Integration tests for {@GrantPrivilegesCommand}.
 */
public final class GrantPrivilegesCommandTest extends PrivilegesTest {
  @Test
  public void grantSinglePrivilege() {
    assertEquals("A: [TTL]\n", run("grant", "-group", "A", "-privileges", "TTL"));
  }

  @Test
  public void grantSinglePrivilegeDifferentArgOrder() {
    assertEquals("A: [TTL]\n", run("grant", "-privileges", "TTL", "-group", "A"));
  }

  @Test
  public void grantMultiplePrivileges() {
    assertEquals("A: [FREE, TTL]\n", run("grant", "-group", "A", "-privileges", "TTL", "FREE"));
  }

  @Test
  public void grantMultiplePrivilegeArgs() {
    assertEquals("A: [FREE, TTL]\n",
        run("grant", "-group", "A", "-privileges", "TTL", "-privileges", "FREE"));
  }

  @Test
  public void grantAdditionalPrivileges() throws Exception {
    grantPrivileges("A", FREE);
    assertEquals("A: [FREE, TTL]\n", run("grant", "-group", "A", "-privileges", "TTL"));
  }

  @Test
  public void caseInsensitive() throws Exception {
    assertEquals("A: [TTL]\n", run("grant", "-group", "A", "-privileges", "tTl"));
  }

  @Test
  public void grantAllPrivileges() throws Exception {
    assertEquals("A: [FREE, PIN, REPLICATION, TTL]\n",
        run("grant", "-group", "A", "-privileges", "all"));
  }

  @Test
  public void grantNoPrivileges() throws Exception {
    assertEquals("A: []\n", run("grant", "-group", "A", "-privileges", "none"));
  }

  @Test
  public void grantAllAndMore() throws Exception {
    assertThat(run("grant", "-group", "A", "-privileges", "ALL", "FREE"),
        containsString("The ALL privilege should be specified on its own"));
  }

  @Test
  public void grantNoneAndMore() throws Exception {
    assertThat(run("grant", "-group", "A", "-privileges", "NONE", "FREE"),
        containsString("The NONE privilege should be specified on its own"));
  }

  @Test
  public void grantNonexistentPrivilege() throws Exception {
    assertThat(run("grant", "-group", "A", "-privileges", "notAPrivilege"),
        containsString("Unrecognized privilege: notAPrivilege"));
  }
}
