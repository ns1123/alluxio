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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.thrift.TProcessor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import alluxio.Configuration;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.jobmanager.AlluxioEEConstants;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.worker.AbstractWorker;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.jobmanager.command.CommandHandlingExecutor;

public final class JobManagerWorker extends AbstractWorker {
  /** BlockWorker handle for access block info. */
  private final BlockWorker mBlockWorker;

  /** Client for job manager master communication */
  private final JobManagerMasterClient mJobManagerMasterClient;
  /** The service that handles commands sent from master */
  private Future<?> mCommandHandlingService;

  /** Configuration object. */
  private final Configuration mConf;

  public JobManagerWorker(BlockWorker blockWorker) {
    super(Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("job-manager-worker-heartbeat-%d", true)));
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mConf = WorkerContext.getConf();
    mJobManagerMasterClient = new JobManagerMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mConf), mConf);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return Maps.newHashMap();
  }

  @Override
  public void start() throws IOException {
    mCommandHandlingService = getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.JOB_MANAGER_WORKER_COMMAND_HANDLING,
            new CommandHandlingExecutor(mJobManagerMasterClient, mBlockWorker),
            mConf.getInt(AlluxioEEConstants.JOB_MANAGER_MASTER_WORKER_HEARTBEAT_INTERVAL_MS)));
  }

  @Override
  public void stop() throws IOException {
    if (mCommandHandlingService != null) {
      mCommandHandlingService.cancel(true);
    }
    mJobManagerMasterClient.close();
    getExecutorService().shutdown();
  }

}
