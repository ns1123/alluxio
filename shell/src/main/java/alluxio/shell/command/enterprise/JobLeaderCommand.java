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

import alluxio.Configuration;
import alluxio.MasterInquireClient;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.shell.command.AbstractShellCommand;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the current leader master host name.
 */
@ThreadSafe
public final class JobLeaderCommand extends AbstractShellCommand {
  private static final Logger LOG = LoggerFactory.getLogger(JobLeaderCommand.class);

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
  public int run(CommandLine cl) {
    if (!Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      System.out.println(Configuration.get(PropertyKey.JOB_MASTER_HOSTNAME));
      return 0;
    }

    String zkAddress = Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS);
    String electionPath = Configuration.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH);
    String leaderPath = Configuration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH);
    MasterInquireClient client = MasterInquireClient.getClient(zkAddress, electionPath, leaderPath);

    String rawAddress = client.getLeaderAddress();
    if (rawAddress != null) {
      try {
        InetSocketAddress inetSocketAddress = NetworkAddressUtils.parseInetSocketAddress(rawAddress);
        System.out.println(inetSocketAddress.getHostName());
      } catch (IOException e) {
        LOG.error("Failed to parse leader address", e);
        System.out.println("Failed to parse leader address: " + e.toString());
      }
    } else {
      System.out.println("Failed to get the hostname of the job master service leader.");
    }
    return 0;
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
