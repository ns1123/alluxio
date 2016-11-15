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

import alluxio.Configuration;
import alluxio.MasterInquireClient;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

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
  protected Options getOptions() {
    Options options = new Options();
    options.addOption(
        Option.builder("zkAddress")
        .required(false)
        .hasArg(true)
        .desc("the address of the zookeeper cluster, e.g. 'node1:2181'")
        .build());
    return options;
  }

  @Override
  public void run(CommandLine cl) {
    if (!Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      System.out.println(Configuration.get(PropertyKey.MASTER_HOSTNAME));
      return;
    }

    String zkAddress = Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS);
    String electionPath = Configuration.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH);
    String leaderPath = Configuration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH);
    MasterInquireClient client = MasterInquireClient.getClient(zkAddress, electionPath, leaderPath);

    String hostname = client.getLeaderAddress();
    if (hostname != null) {
      System.out.println(hostname);
    } else {
      System.out.println("Failed to get the leader job master.");
    }
  }

  @Override
  public String getUsage() {
    return "jobLeader";
  }

  @Override
  public String getDescription() {
    return "Prints the current leader job master hostname.";
  }
}
