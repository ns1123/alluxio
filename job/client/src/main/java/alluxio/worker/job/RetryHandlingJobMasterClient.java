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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.JobCommand;
import alluxio.thrift.JobMasterWorkerService;
import alluxio.thrift.TaskInfo;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import org.apache.thrift.TException;

import java.net.InetSocketAddress;
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
   */
  protected static RetryHandlingJobMasterClient create() {
    if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      return new RetryHandlingJobMasterClient(
          Configuration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH));
    }
    return new RetryHandlingJobMasterClient(
        NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC));
  }

  /**
   * Creates a new job master client.
   *
   * @param masterAddress the master address
   */
  private RetryHandlingJobMasterClient(InetSocketAddress masterAddress) {
    super(null, masterAddress);
  }

  /**
   * Creates a new job master client.
   *
   * @param zkLeaderPath the Zookeeper path for the job master leader address
   */
  private RetryHandlingJobMasterClient(String zkLeaderPath) {
    super(null, zkLeaderPath);
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
  public synchronized long registerWorker(final WorkerNetAddress address) {
    return retryRPC(new RpcCallable<Long>() {
      public Long call() throws TException {
        return mClient.registerWorker(new alluxio.thrift.WorkerNetAddress(address.getHost(),
            address.getRpcPort(), address.getDataPort(), address.getWebPort(),
            address.getSecureRpcPort()));
      }
    });
  }

  @Override
  public synchronized List<JobCommand> heartbeat(final long workerId,
      final List<TaskInfo> taskInfoList) {
    return retryRPC(new RpcCallable<List<JobCommand>>() {

      @Override
      public List<JobCommand> call() throws TException {
        return mClient.heartbeat(workerId, taskInfoList);
      }
    });
  }
}
