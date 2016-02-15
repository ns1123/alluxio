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

package alluxio.jobmanager.worker.command;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.jobmanager.job.persist.DistributedPersistConfig;
import alluxio.jobmanager.worker.JobManagerMasterClient;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.Status;
import alluxio.thrift.TaskStatus;
import alluxio.worker.WorkerIdRegistry;

/**
 * Manages the communication with the master. Dispatches the recevied command from master to
 * handers, and send the status of all the tasks to master.
 */
public class CommandHandlingExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final JobManagerMasterClient mMasterClient;

  public CommandHandlingExecutor(JobManagerMasterClient masterClient) {
    mMasterClient = Preconditions.checkNotNull(masterClient);
  }

  @Override
  public void heartbeat() {
    List<TaskStatus> taskStatusList = Lists.newArrayList();

    // example test
    TaskStatus status = new TaskStatus();
    status.setStatus(Status.INPROGRESS);
    status.setJobId(1L);
    status.setTaskId(2);
    taskStatusList.add(status);

    List<JobManangerCommand> commands = null;
    try {
      commands = mMasterClient.heartbeat(WorkerIdRegistry.getWorkerId(), taskStatusList);
    } catch (AlluxioException | IOException e) {
      LOG.error("Failed to heartbeat to master", e);
      return;
    }

    for (JobManangerCommand command : commands) {
      byte[] bytes = command.getRunTaskCommand().getJobConfig();
      byte[] bytes1 = command.getRunTaskCommand().getTaskArgs();
      try {
        DistributedPersistConfig jobConfig = (DistributedPersistConfig) deserialize(bytes);
        List<Long> args = (List<Long>) deserialize(bytes1);
        LOG.info("job config:" + jobConfig);
        LOG.info("args:" + args);
      } catch (ClassNotFoundException | IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      LOG.info(command.toString());
    }
  }

  public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
      try (ObjectInputStream o = new ObjectInputStream(b)) {
        return o.readObject();
      }
    }
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }
}
