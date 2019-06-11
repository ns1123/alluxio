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

package alluxio.cli.fs.policy;

import alluxio.cli.CommandUtils;
import alluxio.cli.fs.command.AbstractFileSystemCommand;
import alluxio.client.file.FileSystemContext;
import alluxio.client.policy.PolicyMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.RemovePolicyPOptions;
import alluxio.master.MasterClientContext;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Removes a policy from the alluxio file system.
 */
@ThreadSafe
public final class RemovePolicyCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public RemovePolicyCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "remove";
  }

  @Override
  public Options getOptions() {
    return new Options();
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String policyName = cl.getArgs()[0];
    PolicyMasterClient policyClient = PolicyMasterClient.Factory
        .create(MasterClientContext.newBuilder(mFsContext.getClientContext()).build());

    policyClient.removePolicy(policyName, RemovePolicyPOptions.getDefaultInstance());
    System.out.println("Removed policy");
    return 0;
  }

  @Override
  public String getUsage() {
    return "remove <policy name>";
  }

  @Override
  public String getDescription() {
    return "removes a policy definition from Alluxio";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
