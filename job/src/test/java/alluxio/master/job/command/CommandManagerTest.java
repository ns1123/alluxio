/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job.command;

import alluxio.job.JobConfig;
import alluxio.job.load.LoadConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.thrift.JobManangerCommand;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link CommandManager}.
 */
public final class CommandManagerTest {
  private CommandManager mManager;

  @Before
  public void before() {
    mManager = new CommandManager();
  }

  @Test
  public void submitRunTaskCommandTest() throws Exception {
    long jobId = 0L;
    int taskId = 1;
    JobConfig jobConfig = new LoadConfig("/test");
    long workerId = 2L;
    List<Integer> args = Lists.newArrayList(1);
    mManager.submitRunTaskCommand(jobId, taskId, jobConfig, args, workerId);
    List<JobManangerCommand> commands = mManager.pollAllPendingCommands(workerId);
    Assert.assertEquals(1, commands.size());
    JobManangerCommand command = commands.get(0);
    Assert.assertEquals(jobId, command.getRunTaskCommand().getJobId());
    Assert.assertEquals(taskId, command.getRunTaskCommand().getTaskId());
    Assert.assertEquals(jobConfig,
        SerializationUtils.deserialize(command.getRunTaskCommand().getJobConfig()));
    Assert.assertEquals(args,
        SerializationUtils.deserialize(command.getRunTaskCommand().getTaskArgs()));
  }

  @Test
  public void submitCancelTaskCommandTest() {
    long jobId = 0L;
    int taskId = 1;
    long workerId = 2L;
    mManager.submitCancelTaskCommand(jobId, taskId, workerId);
    List<JobManangerCommand> commands = mManager.pollAllPendingCommands(workerId);
    Assert.assertEquals(1, commands.size());
    JobManangerCommand command = commands.get(0);
    Assert.assertEquals(jobId, command.getCancelTaskCommand().getJobId());
    Assert.assertEquals(taskId, command.getCancelTaskCommand().getTaskId());
  }
}
