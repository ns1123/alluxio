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
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.JobMasterWorkerService.Iface;
import alluxio.thrift.JobCommand;
import alluxio.thrift.TaskInfo;
import alluxio.thrift.WorkerNetAddress;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a Thrift handler for job master RPCs invoked by an Alluxio worker.
 */
@ThreadSafe
public final class JobMasterWorkerServiceHandler implements Iface {
  private final JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterWorkerServiceHandler}.
   *
   * @param JobMaster the {@link JobMaster} that the handler uses internally
   */
  public JobMasterWorkerServiceHandler(JobMaster JobMaster) {
    mJobMaster = Preconditions.checkNotNull(JobMaster);
  }

  @Override
  public synchronized long getServiceVersion() throws TException {
    return Constants.JOB_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  public long getWorkerId(WorkerNetAddress workerNetAddress) {
    return mJobMaster.getWorkerId(ThriftUtils.fromThrift((workerNetAddress)));
  }

  @Override
  public synchronized List<JobCommand> heartbeat(long workerId, List<TaskInfo> taskInfoList)
      throws AlluxioTException, TException {
    List<alluxio.job.wire.TaskInfo> wireTaskInfoList = Lists.newArrayList();
    for (TaskInfo taskInfo : taskInfoList) {
      wireTaskInfoList.add(new alluxio.job.wire.TaskInfo(taskInfo));
    }
    return mJobMaster.workerHeartbeat(workerId, wireTaskInfoList);
  }

  @Override
  public void registerWorker(long workerId) throws AlluxioTException {
    try {
      mJobMaster.workerRegister(workerId);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }
}
