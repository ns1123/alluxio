/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.client.job;

import alluxio.AbstractMasterClient;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.JobInfo;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.JobMasterClientService;
import alluxio.thrift.ThriftIOException;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the job service master, used by job service
 * clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RetryHandlingJobMasterClient extends AbstractMasterClient
    implements JobMasterClient {
  private JobMasterClientService.Client mClient = null;

  /**
   * Creates a new job master client.
   */
  protected static RetryHandlingJobMasterClient create() {
    if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      return new RetryHandlingJobMasterClient(
          Configuration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH));
    } else {
      return new RetryHandlingJobMasterClient(
          NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC));
    }
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
  public RetryHandlingJobMasterClient(String zkLeaderPath) {
    super(null, zkLeaderPath);
  }

  @Override
  protected Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.JOB_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.JOB_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new JobMasterClientService.Client(mProtocol);
  }

  @Override
  public synchronized void cancel(final long jobId)
      throws AlluxioException {
    try {
      retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
        public Void call() throws AlluxioTException, TException {
          mClient.cancel(jobId);
          return null;
        }
      });
    } catch (IOException e) {
      throw new UnexpectedAlluxioException(e.getMessage());
    }
  }

  @Override
  public synchronized JobInfo getStatus(final long jobId)
      throws AlluxioException {
    try {
      return retryRPC(new RpcCallableThrowsAlluxioTException<JobInfo>() {
        public JobInfo call() throws AlluxioTException, TException {
          try {
            return new JobInfo(mClient.getStatus(jobId));
          } catch (ClassNotFoundException | IOException e) {
            throw new ThriftIOException(e.getMessage());
          }
        }
      });
    } catch (IOException e) {
      throw new UnexpectedAlluxioException(e.getMessage());
    }
  }

  @Override
  public synchronized List<Long> list()
      throws AlluxioException {
    try {
      return retryRPC(new RpcCallableThrowsAlluxioTException<List<Long>>() {
        public List<Long> call() throws AlluxioTException, TException {
          return mClient.listAll();
        }
      });
    } catch (IOException e) {
      throw new UnexpectedAlluxioException(e.getMessage());
    }
  }

  @Override
  public synchronized long run(final JobConfig jobConfig)
      throws AlluxioException {
    try {
      return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
        public Long call() throws AlluxioTException, TException {
          try {
            return mClient.run(ByteBuffer.wrap(SerializationUtils.serialize(jobConfig)));
          } catch (IOException e) {
            throw new ThriftIOException(e.getMessage());
          }
        }
      });
    } catch (IOException e) {
      throw new UnexpectedAlluxioException(e.getMessage());
    }
  }
}
