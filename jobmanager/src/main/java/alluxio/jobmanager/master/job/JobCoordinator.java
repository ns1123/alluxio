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

package alluxio.jobmanager.master.job;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import alluxio.jobmanager.job.JobConfig;
import alluxio.jobmanager.job.JobDefinition;
import alluxio.jobmanager.job.JobDefinitionRegistry;
import alluxio.jobmanager.master.command.CommandManager;
import alluxio.master.block.BlockMaster;
import alluxio.wire.WorkerInfo;

public final class JobCoordinator {
  private final JobInfo mJobInfo;
  private final CommandManager mCommandManager;
  private final BlockMaster mBlockMaster;
  private Map<Integer, Long> mTaskIdToWorkerId;

  private JobCoordinator(JobInfo jobInfo, BlockMaster blockMaster) {
    mJobInfo = Preconditions.checkNotNull(jobInfo);
    mCommandManager = CommandManager.ISNTANCE;
    mBlockMaster = Preconditions.checkNotNull(blockMaster);
    mTaskIdToWorkerId = Maps.newHashMap();
  }

  public static JobCoordinator create(JobInfo jobInfo, BlockMaster blockMaster) {
    JobCoordinator jobCoordinator = new JobCoordinator(jobInfo, blockMaster);
    jobCoordinator.start();
    // start the coordinator, create the tasks
    return jobCoordinator;
  }

  private synchronized void start() {
    // get the job definition
    JobDefinition<JobConfig, ?> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());
    List<WorkerInfo> workerInfoList = mBlockMaster.getWorkerInfoList();
    Map<WorkerInfo, ?> taskAddressToArgs =
        definition.selectExecutors(mJobInfo.getJobConfig(), workerInfoList);

    for (Entry<WorkerInfo, ?> entry : taskAddressToArgs.entrySet()) {
      int taskId = mTaskIdToWorkerId.size();
      // create task
      mJobInfo.addTask(taskId);
      // submit commands
      mCommandManager.submitRunTaskCommand(mJobInfo.getId(), taskId, mJobInfo.getJobConfig(),
          entry.getValue(), entry.getKey().getId());
      mTaskIdToWorkerId.put(taskId, entry.getKey().getId());
    }
  }

  public void cancel() {
    for (int taskId : mJobInfo.getTaskIdList()) {
      mCommandManager.submitCancelTaskCommand(mJobInfo.getId(), taskId,
          mTaskIdToWorkerId.get(taskId));
    }
  }
}
