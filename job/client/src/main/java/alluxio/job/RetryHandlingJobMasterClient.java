/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.JobCommand;
import alluxio.thrift.JobMasterWorkerService;
import alluxio.thrift.TaskInfo;
import alluxio.wire.WorkerNetAddress;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the job service master, used by job workers.
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
   * @param masterAddress the master address
   */
  public RetryHandlingJobMasterClient(InetSocketAddress masterAddress) {
    super(masterAddress);
  }

  /**
   * Creates a new job master client.
   *
   * @param zkLeaderPath the Zookeeper path for the job master leader address
   */
  public RetryHandlingJobMasterClient(String zkLeaderPath) {
    super(zkLeaderPath);
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
  protected void afterConnect() throws IOException {
    mClient = new JobMasterWorkerService.Client(mProtocol);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized long registerWorker(final WorkerNetAddress address)
      throws IOException, ConnectionFailedException {
    return retryRPC(new RpcCallable<Long>() {
      public Long call() throws TException {
        return mClient.registerWorker(new alluxio.thrift.WorkerNetAddress(address.getHost(),
            address.getRpcPort(), address.getDataPort(), address.getWebPort(),
            address.getSecureRpcPort()));
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized List<JobCommand> heartbeat(final long workerId,
      final List<TaskInfo> taskInfoList) throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<List<JobCommand>>() {

      @Override
      public List<JobCommand> call() throws AlluxioTException, TException {
        return mClient.heartbeat(workerId, taskInfoList);
      }
    });
  }
}
