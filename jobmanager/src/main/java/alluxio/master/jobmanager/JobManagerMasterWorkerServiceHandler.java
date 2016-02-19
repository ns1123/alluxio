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

package alluxio.master.jobmanager;

import java.util.List;

import org.apache.thrift.TException;

import com.google.common.base.Preconditions;

import alluxio.jobmanager.AlluxioEEConstants;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.JobManagerMasterWorkerService.Iface;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.TaskInfo;

public class JobManagerMasterWorkerServiceHandler implements Iface {
  private final JobManagerMaster mJobManagerMaster;

  public JobManagerMasterWorkerServiceHandler(JobManagerMaster JobManagerMaster) {
    mJobManagerMaster = Preconditions.checkNotNull(JobManagerMaster);
  }

  @Override
  public long getServiceVersion() throws TException {
    return AlluxioEEConstants.JOB_MANAGER_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  public List<JobManangerCommand> heartbeat(long workerId, List<TaskInfo> taskInfoList)
      throws AlluxioTException, TException {
    return mJobManagerMaster.workerHeartbeat(workerId, taskInfoList);
  }

}
