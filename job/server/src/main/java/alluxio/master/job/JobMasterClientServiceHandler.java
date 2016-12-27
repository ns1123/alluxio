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
import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.JobInfo;
import alluxio.thrift.JobMasterClientService;
import alluxio.thrift.ThriftIOException;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class is a Thrift handler for job master RPCs invoked by a service client.
 */
public class JobMasterClientServiceHandler implements JobMasterClientService.Iface {
  private JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterClientRestServiceHandler}.
   *
   * @param jobMaster the job master to use
   */
  public JobMasterClientServiceHandler(JobMaster jobMaster) {
    Preconditions.checkNotNull(jobMaster);
    mJobMaster = jobMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.JOB_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public void cancel(final long id) throws AlluxioTException {
    RpcUtils.call(new RpcUtils.RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mJobMaster.cancelJob(id);
        return null;
      }
    });
  }

  @Override
  public JobInfo getStatus(final long id) throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(new RpcUtils.RpcCallableThrowsIOException<JobInfo>() {
      @Override
      public JobInfo call() throws AlluxioException, IOException {
        return mJobMaster.getJobInfo(id).toThrift();
      }
    });
  }

  @Override
  public List<Long> listJobs() throws AlluxioTException {
    return RpcUtils.call(new RpcUtils.RpcCallable<List<Long>>() {
      @Override
      public List<Long> call() throws AlluxioException {
        return mJobMaster.listJobs();
      }
    });
  }

  @Override
  public long run(final ByteBuffer jobConfig) throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(new RpcUtils.RpcCallableThrowsIOException<Long>() {
      @Override
      public Long call() throws AlluxioException, IOException {
        try {
          return mJobMaster.runJob((JobConfig) SerializationUtils.deserialize(jobConfig.array()));
        } catch (ClassNotFoundException e) {
          throw new IOException(e.getMessage());
        }
      }
    });
  }
}
