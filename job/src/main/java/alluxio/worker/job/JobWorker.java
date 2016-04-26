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
import alluxio.exception.ConnectionFailedException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.wire.WorkerNetAddress;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.worker.AbstractWorker;
import alluxio.worker.WorkerContext;
import alluxio.worker.JobWorkerIdRegistry;
import alluxio.worker.job.command.CommandHandlingExecutor;
import alluxio.worker.job.task.TaskExecutorManager;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job worker that manages all the worker-related activities.
 */
@NotThreadSafe
public final class JobWorker extends AbstractWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Client for job master communication. */
  private final JobMasterClient mJobMasterClient;
  /** The manager for the all the local task execution. */
  private final TaskExecutorManager mTaskExecutorManager;
  /** The service that handles commands sent from master. */
  private Future<?> mCommandHandlingService;

  /** Configuration object. */
  private final Configuration mConf;

  /**
   * Creates a new instance of {@link JobWorker}.
   */
  public JobWorker() {
    super(Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("job-worker-heartbeat-%d", true)));
    mConf = WorkerContext.getConf();
    mJobMasterClient = new JobMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RPC, mConf), mConf);
    mTaskExecutorManager = new TaskExecutorManager();
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return Maps.newHashMap();
  }

  @Override
  public void start() throws IOException {
    try {
      WorkerNetAddress netAddress = WorkerContext.getNetAddress();
      JobWorkerIdRegistry.registerWithJobMaster(mJobMasterClient, netAddress);
    } catch (ConnectionFailedException e) {
      LOG.error("Failed to get a worker id from job master", e);
      throw Throwables.propagate(e);
    }

    mCommandHandlingService = getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.JOB_WORKER_COMMAND_HANDLING,
            new CommandHandlingExecutor(mTaskExecutorManager, mJobMasterClient),
            mConf.getInt(Constants.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL_MS)));
  }

  @Override
  public void stop() throws IOException {
    if (mCommandHandlingService != null) {
      mCommandHandlingService.cancel(true);
    }
    mJobMasterClient.close();
    getExecutorService().shutdown();
  }
}
