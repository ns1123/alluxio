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
import alluxio.grpc.ListPolicyPOptions;
import alluxio.grpc.PolicyInfo;
import alluxio.master.MasterClientContext;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Lists policies for the Alluxio fs.
 */
@ThreadSafe
public final class ListPolicyCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public ListPolicyCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "list";
  }

  @Override
  public Options getOptions() {
    return new Options();
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    PolicyMasterClient policyClient = PolicyMasterClient.Factory
        .create(MasterClientContext.newBuilder(mFsContext.getClientContext()).build());

    List<PolicyInfo> policies = policyClient.listPolicy(ListPolicyPOptions.getDefaultInstance());
    for (PolicyInfo pi : policies) {
      System.out.println(pi);
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "list";
  }

  @Override
  public String getDescription() {
    return "Lists the policy definitions.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }
}
