/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.file;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.FileSystemMasterJobService;
import alluxio.thrift.GetFileInfoTOptions;
import alluxio.thrift.GetUfsInfoTOptions;
import alluxio.thrift.UfsInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.ThriftUtils;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the file system master, used by Alluxio job
 * service.
 * <p/>
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class FileSystemMasterClient extends AbstractMasterClient {
  private FileSystemMasterJobService.Client mClient = null;

  /**
   * Creates a instance of {@link FileSystemMasterClient}.
   *
   * @param masterAddress the master address
   */
  public FileSystemMasterClient(InetSocketAddress masterAddress) {
    super(null, masterAddress);
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_JOB_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_JOB_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new FileSystemMasterJobService.Client(mProtocol);
  }

  /**
   * @param fileId the id of the file for which to get the {@link FileInfo}
   * @return the file info for the given file id
   */
  public synchronized FileInfo getFileInfo(final long fileId) throws IOException {
    return retryRPC(new RpcCallable<FileInfo>() {
      @Override
      public FileInfo call() throws TException {
        return ThriftUtils
            .fromThrift(mClient.getFileInfo(fileId, new GetFileInfoTOptions()).getFileInfo());
      }
    });
  }

  /**
   * @param mountId the id of the mount
   * @return the ufs information for the give ufs
   * @throws IOException if an I/O error occurs
   */
  public synchronized UfsInfo getUfsInfo(final long mountId) throws IOException {
    return retryRPC(new RpcCallable<UfsInfo>() {
      @Override
      public UfsInfo call() throws TException {
        return mClient.getUfsInfo(mountId, new GetUfsInfoTOptions()).getUfsInfo();
      }
    });
  }
}
