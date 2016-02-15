/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.jobmanager.master.command;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import alluxio.jobmanager.job.JobConfig;
import alluxio.jobmanager.util.SerializationUtils;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.RunTaskCommand;

public enum CommandManager {
  ISNTANCE;

  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  // TODO(yupeng) add retry support
  private final Map<Long, List<JobManangerCommand>> mWorkerIdToPendingCommands;

  private CommandManager() {
    mWorkerIdToPendingCommands = Maps.newHashMap();
  }

  public synchronized void submitRunTaskCommand(long jobId, int taskId, JobConfig jobConfig,
      Object taskArgs, long workerId) {
    RunTaskCommand runTaskCommand = new RunTaskCommand();
    runTaskCommand.setJobId(jobId);
    runTaskCommand.setTaskId(taskId);
    try {
      runTaskCommand.setJobConfig(SerializationUtils.serialize(jobConfig));
      runTaskCommand.setTaskArgs(SerializationUtils.serialize(taskArgs));
    } catch (IOException e) {
      // TODO(yupeng) better exception handling
      LOG.info("Failed to serialize the run task command:" + e);
      return;
    }
    JobManangerCommand command = new JobManangerCommand();
    command.setRunTaskCommand(runTaskCommand);
    if (!mWorkerIdToPendingCommands.containsKey(workerId)) {
      mWorkerIdToPendingCommands.put(workerId, Lists.<JobManangerCommand>newArrayList());
    }
    mWorkerIdToPendingCommands.get(workerId).add(command);
  }

  public synchronized List<JobManangerCommand> pollAllPendingCommands(long workerId) {
    if (!mWorkerIdToPendingCommands.containsKey(workerId)) {
      return Lists.newArrayList();
    }
    List<JobManangerCommand> commands =
        Lists.newArrayList(mWorkerIdToPendingCommands.get(workerId));
    mWorkerIdToPendingCommands.get(workerId).clear();
    return commands;
  }
}
