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

package alluxio.cli.fs.command;

import alluxio.cli.Command;
import alluxio.cli.fs.policy.AddPolicyCommand;
import alluxio.cli.fs.policy.ListPolicyCommand;
import alluxio.cli.fs.policy.RemovePolicyCommand;
import alluxio.client.file.FileSystemContext;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Mounts a UFS path onto an Alluxio path.
 */
@ThreadSafe
public final class PolicyCommand extends AbstractFileSystemCommand {

  private final Map<String, Command> mSubCommands = new HashMap<>();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public PolicyCommand(FileSystemContext fsContext) {
    super(fsContext);
    mSubCommands.put("add", new AddPolicyCommand(fsContext));
    mSubCommands.put("list", new ListPolicyCommand(fsContext));
    mSubCommands.put("remove", new RemovePolicyCommand(fsContext));
  }

  @Override
  public String getCommandName() {
    return "policy";
  }

  @Override
  public boolean hasSubCommand() {
    return true;
  }

  @Override
  public Map<String, Command> getSubCommands() {
    return mSubCommands;
  }

  @Override
  public String getUsage() {
    StringBuilder usage = new StringBuilder(getCommandName());
    for (String cmd : mSubCommands.keySet()) {
      usage.append(" [").append(cmd).append("]");
    }
    return usage.toString();
  }

  @Override
  public String getDescription() {
    return "Manage policies for the filesystem";
  }
}
