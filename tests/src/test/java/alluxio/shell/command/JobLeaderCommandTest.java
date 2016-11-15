/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.shell.command;

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
    String expected = mLocalAlluxioCluster.getMaster().getAddress().getHostName() + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
