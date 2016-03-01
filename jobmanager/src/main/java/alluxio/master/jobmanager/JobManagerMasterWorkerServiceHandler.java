/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.jobmanager;

import alluxio.Constants;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.JobManagerMasterWorkerService.Iface;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.TaskInfo;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a Thrift handler for job manager master RPCs invoked by an Alluxio worker.
 */
@ThreadSafe
public final class JobManagerMasterWorkerServiceHandler implements Iface {
  private final JobManagerMaster mJobManagerMaster;

  /**
   * Creates a new instance of {@link JobManagerMasterWorkerServiceHandler}.
   *
   * @param JobManagerMaster the {@link JobManagerMaster} that the handler uses internally
   */
  public JobManagerMasterWorkerServiceHandler(JobManagerMaster JobManagerMaster) {
    mJobManagerMaster = Preconditions.checkNotNull(JobManagerMaster);
  }

  @Override
  public synchronized long getServiceVersion() throws TException {
    return Constants.JOB_MANAGER_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  public synchronized List<JobManangerCommand> heartbeat(long workerId, List<TaskInfo> taskInfoList)
      throws AlluxioTException, TException {
    return mJobManagerMaster.workerHeartbeat(workerId, taskInfoList);
  }

}
