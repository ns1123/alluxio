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
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CancelTOptions;
import alluxio.thrift.CancelTResponse;
import alluxio.thrift.GetJobStatusTOptions;
import alluxio.thrift.GetJobStatusTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.JobMasterClientService;
import alluxio.thrift.ListAllTOptions;
import alluxio.thrift.ListAllTResponse;
import alluxio.thrift.RunTOptions;
import alluxio.thrift.RunTResponse;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is a Thrift handler for job master RPCs invoked by a job service client.
 */
public class JobMasterClientServiceHandler implements JobMasterClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(JobMasterClientServiceHandler.class);
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
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.JOB_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public CancelTResponse cancel(final long id, CancelTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<CancelTResponse>() {
      @Override
      public CancelTResponse call() throws AlluxioException {
        mJobMaster.cancel(id);
        return new CancelTResponse();
      }
    });
  }

  @Override
  public GetJobStatusTResponse getJobStatus(final long id, GetJobStatusTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallableThrowsIOException<GetJobStatusTResponse>() {
      @Override
      public GetJobStatusTResponse call() throws AlluxioException, IOException {
        return new GetJobStatusTResponse(mJobMaster.getStatus(id).toThrift());
      }
    });
  }

  @Override
  public ListAllTResponse listAll(ListAllTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<ListAllTResponse>() {
      @Override
      public ListAllTResponse call() throws AlluxioException {
        return new ListAllTResponse(mJobMaster.list());
      }
    });
  }

  @Override
  public RunTResponse run(final ByteBuffer jobConfig, RunTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallableThrowsIOException<RunTResponse>() {
      @Override
      public RunTResponse call() throws AlluxioException, IOException {
        try {
          return new RunTResponse(
              mJobMaster.run((JobConfig) SerializationUtils.deserialize(jobConfig.array())));
        } catch (ClassNotFoundException e) {
          throw new InvalidArgumentException(e);
        }
      }
    });
  }
}
