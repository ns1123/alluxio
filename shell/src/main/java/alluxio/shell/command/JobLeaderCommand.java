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

package alluxio.shell.command;

import alluxio.client.file.FileSystem;
import alluxio.job.util.JobRestClientUtils;

import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the current leader master host name.
 */
@ThreadSafe
public final class JobLeaderCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public JobLeaderCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "jobLeader";
  }

  @Override
  public int getNumOfArgs() {
    return 0;
  }

  @Override
  public void run(CommandLine cl) {
    System.out.println(JobRestClientUtils.getJobMasterAddress().getHostName());
  }

  @Override
  public String getUsage() {
    return "jobLeader";
  }

  @Override
  public String getDescription() {
    return "Prints the hostname of the job master service leader.";
  }
}
