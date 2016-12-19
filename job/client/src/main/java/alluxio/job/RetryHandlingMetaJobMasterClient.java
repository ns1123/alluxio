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
import alluxio.exception.ConnectionFailedException;
import alluxio.job.wire.JobMasterInfo;
import alluxio.job.wire.JobMasterInfo.JobMasterInfoField;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.MetaJobMasterClientService;

import com.google.common.base.Throwables;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the meta job master.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RetryHandlingMetaJobMasterClient extends AbstractMasterClient
    implements MetaJobMasterClient {
  private MetaJobMasterClientService.Client mClient;

  /**
   * Creates a new meta job master client.
   *
   * @param jobMasterAddress the JobMaster address
   */
  public RetryHandlingMetaJobMasterClient(InetSocketAddress jobMasterAddress) {
    super(jobMasterAddress);
  }

  /**
   * Creates a new meta job master client.
   *
   * @param zkLeaderPath the leader path for looking up the master address
   */
  public RetryHandlingMetaJobMasterClient(String zkLeaderPath) {
    super(zkLeaderPath);
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.META_JOB_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.META_JOB_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new MetaJobMasterClientService.Client(mProtocol);
  }

  @Override
  public synchronized JobMasterInfo getInfo(final Set<JobMasterInfoField> fields)
      throws ConnectionFailedException {
    try {
      return retryRPC(new RpcCallable<JobMasterInfo>() {
        @Override
        public JobMasterInfo call() throws TException {
          Set<alluxio.thrift.JobMasterInfoField> thriftFields = new HashSet<>();
          if (fields == null) {
            thriftFields = null;
          } else {
            for (JobMasterInfoField field : fields) {
              thriftFields.add(field.toThrift());
            }
          }
          return JobMasterInfo.fromThrift(mClient.getInfo(thriftFields));
        }
      });
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
