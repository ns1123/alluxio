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

package alluxio.worker.jobmanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;

import alluxio.AbstractMasterClient;
import alluxio.Configuration;
import alluxio.EnterpriseConstants;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.JobManagerMasterWorkerService;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.TaskInfo;

/**
 * A wrapper for the thrift client to interact with the job service master, used by job manager
 * workers.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class JobManagerMasterClient extends AbstractMasterClient {

  private JobManagerMasterWorkerService.Client mClient = null;

  /**
   * Creates a new job manager master client.
   *
   * @param masterAddress the master address
   * @param configuration the Alluxio configuration
   */
  public JobManagerMasterClient(InetSocketAddress masterAddress, Configuration configuration) {
    super(masterAddress, configuration);
  }

  @Override
  protected Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return EnterpriseConstants.JOB_MANAGER_MASTER_WORKER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return EnterpriseConstants.JOB_MANAGER_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new JobManagerMasterWorkerService.Client(mProtocol);
  }

  /**
   * Periodic heartbeats to update the tasks' status from a worker, and returns the commands.
   *
   * @param workerId the worker id
   * @param taskInfoList the list of the task information
   * @return the commands issued to the worker
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized List<JobManangerCommand> heartbeat(final long workerId,
      final List<TaskInfo> taskInfoList) throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<List<JobManangerCommand>>() {

      @Override
      public List<JobManangerCommand> call() throws AlluxioTException, TException {
        return mClient.heartbeat(workerId, taskInfoList);
      }
    });
  }
}
