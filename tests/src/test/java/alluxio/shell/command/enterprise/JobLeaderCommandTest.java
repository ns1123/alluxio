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

package alluxio.shell.command.enterprise;

import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the jobLeader shell command.
 */
public final class JobLeaderCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void jobLeader() {
    mFsShell.run("jobLeader");
    String expected =
        mLocalAlluxioCluster.getLocalAlluxioMaster().getAddress().getHostName() + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
