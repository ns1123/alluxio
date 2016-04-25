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
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.JobMasterWorkerService;
import alluxio.thrift.JobCommand;
import alluxio.thrift.TaskInfo;

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
public final class JobMasterClient extends AbstractMasterClient {

  private JobMasterWorkerService.Client mClient = null;

  /**
   * Creates a new job master client.
   *
   * @param masterAddress the master address
   * @param configuration the Alluxio configuration
   */
  public JobMasterClient(InetSocketAddress masterAddress, Configuration configuration) {
    super(masterAddress, configuration);
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
   * Periodic heartbeats to update the tasks' status from a worker, and returns the commands.
   *
   * @param workerId the worker id
   * @param taskInfoList the list of the task information
   * @return the commands issued to the worker
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
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
