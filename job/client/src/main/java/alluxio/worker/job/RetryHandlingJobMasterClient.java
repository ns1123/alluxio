/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker.job;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.JobCommand;
import alluxio.thrift.JobHeartbeatTOptions;
import alluxio.thrift.JobMasterWorkerService;
import alluxio.thrift.RegisterJobWorkerTOptions;
import alluxio.thrift.TaskInfo;
import alluxio.wire.WorkerNetAddress;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the job service master, used by job service
 * workers.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RetryHandlingJobMasterClient extends AbstractMasterClient
    implements JobMasterClient {
  private JobMasterWorkerService.Client mClient = null;

  /**
   * Creates a new job master client.
   *
   * @param conf job master client configuration
   */
  public RetryHandlingJobMasterClient(JobMasterClientConfig conf) {
    super(conf);
  }

  @Override
  protected Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.JOB_MASTER_WORKER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.JOB_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = new JobMasterWorkerService.Client(mProtocol);
  }

  @Override
  public synchronized long registerWorker(final WorkerNetAddress address) throws IOException {
    return retryRPC(new RpcCallable<Long>() {
      public Long call() throws TException {
        return mClient
            .registerJobWorker(address.toThrift(), new RegisterJobWorkerTOptions())
            .getId();
      }
    });
  }

  @Override
  public synchronized List<JobCommand> heartbeat(final long workerId,
      final List<TaskInfo> taskInfoList) throws IOException {
    return retryRPC(new RpcCallable<List<JobCommand>>() {

      @Override
      public List<JobCommand> call() throws TException {
        return mClient.heartbeat(workerId, taskInfoList, new JobHeartbeatTOptions()).getCommands();
      }
    });
  }
}
