/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.exception.AlluxioException;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.JobHeartbeatTOptions;
import alluxio.thrift.JobHeartbeatTResponse;
import alluxio.thrift.JobMasterWorkerService.Iface;
import alluxio.thrift.RegisterJobWorkerTOptions;
import alluxio.thrift.RegisterJobWorkerTResponse;
import alluxio.thrift.TaskInfo;
import alluxio.thrift.WorkerNetAddress;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a Thrift handler for job master RPCs invoked by a job service worker.
 */
@ThreadSafe
public final class JobMasterWorkerServiceHandler implements Iface {
  private static final Logger LOG = LoggerFactory.getLogger(JobMasterWorkerServiceHandler.class);
  private final JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterWorkerServiceHandler}.
   *
   * @param JobMaster the {@link JobMaster} that the handler uses internally
   */
  JobMasterWorkerServiceHandler(JobMaster JobMaster) {
    mJobMaster = Preconditions.checkNotNull(JobMaster);
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.JOB_MASTER_WORKER_SERVICE_VERSION);
  }

  @Override
  public RegisterJobWorkerTResponse registerJobWorker(final WorkerNetAddress workerNetAddress,
      RegisterJobWorkerTOptions options) throws TException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<RegisterJobWorkerTResponse>() {
      @Override
      public RegisterJobWorkerTResponse call() throws AlluxioException {
        return new RegisterJobWorkerTResponse(
            mJobMaster.registerWorker(ThriftUtils.fromThrift((workerNetAddress))));
      }
    });
  }

  @Override
  public synchronized JobHeartbeatTResponse heartbeat(final long workerId,
      final List<TaskInfo> taskInfoList, JobHeartbeatTOptions options) throws TException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<JobHeartbeatTResponse>() {
      @Override
      public JobHeartbeatTResponse call() throws AlluxioException {
        List<alluxio.job.wire.TaskInfo> wireTaskInfoList = Lists.newArrayList();
        for (TaskInfo taskInfo : taskInfoList) {
          try {
            wireTaskInfoList.add(new alluxio.job.wire.TaskInfo(taskInfo));
          } catch (IOException e) {
            LOG.error("task info deserialization failed " + e);
          }
        }
        return new JobHeartbeatTResponse(mJobMaster.workerHeartbeat(workerId, wireTaskInfoList));
      }
    });
  }
}
