/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker.job;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.worker.AbstractWorker;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.job.command.CommandHandlingExecutor;
import alluxio.worker.job.task.TaskExecutorManager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job manager worker that manages all the worker-related activities.
 */
@NotThreadSafe
public final class JobManagerWorker extends AbstractWorker {
  /** BlockWorker handle for access block info. */
  private final BlockWorker mBlockWorker;

  /** Client for job manager master communication. */
  private final JobManagerMasterClient mJobManagerMasterClient;
  private final TaskExecutorManager mTaskExecutorManager;
  /** The service that handles commands sent from master. */
  private Future<?> mCommandHandlingService;

  /** Configuration object. */
  private final Configuration mConf;

  /**
   * Creates a new instance of {@link JobManagerWorker}.
   *
   * @param blockWorker the {@link BlockWorker} in Alluxio
   */
  public JobManagerWorker(BlockWorker blockWorker) {
    super(Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("job-manager-worker-heartbeat-%d", true)));
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mConf = WorkerContext.getConf();
    mJobManagerMasterClient = new JobManagerMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mConf), mConf);
    mTaskExecutorManager = new TaskExecutorManager();
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return Maps.newHashMap();
  }

  @Override
  public void start() throws IOException {
    mCommandHandlingService = getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.JOB_MANAGER_WORKER_COMMAND_HANDLING,
            new CommandHandlingExecutor(mTaskExecutorManager, mJobManagerMasterClient,
                mBlockWorker),
            mConf.getInt(Constants.JOB_MANAGER_MASTER_WORKER_HEARTBEAT_INTERVAL_MS)));
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
