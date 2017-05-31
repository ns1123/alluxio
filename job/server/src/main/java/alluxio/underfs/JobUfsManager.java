/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs;

import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.FileSystemMasterClient;
import alluxio.thrift.UfsInfo;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of UfsManager to manage the ufs used by different job service processes.
 */
// TODO(jiri): Avoid duplication of logic with WorkerUfsManager.
@ThreadSafe
public final class JobUfsManager extends AbstractUfsManager {
  private static final Logger LOG = LoggerFactory.getLogger(JobUfsManager.class);

  private final FileSystemMasterClient mMasterClient;

  /**
   * Constructs an instance of {@link JobUfsManager}.
   */
  public JobUfsManager() {
    mMasterClient = mCloser.register(new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC)));
  }

  @Override
  public UnderFileSystem get(long mountId) throws NotFoundException, UnavailableException {
    try {
      return super.get(mountId);
    } catch (NotFoundException e) {
      // Not cached locally, let's query master
    }

    UfsInfo info;
    try {
      info = mMasterClient.getUfsInfo(mountId);
    } catch (IOException e) {
      throw new UnavailableException(
          String.format("Failed to get UFS info for mount point with id %d", mountId), e);
    }
    Preconditions.checkState((info.isSetUri() && info.isSetProperties()), "unknown mountId");
    UnderFileSystem ufs = super.addMount(mountId, info.getUri(),
        UnderFileSystemConfiguration.defaults().setReadOnly(info.getProperties().isReadOnly())
            .setShared(info.getProperties().isShared())
            .setUserSpecifiedConf(info.getProperties().getProperties()));
    try {
      ufs.connectFromWorker(
          NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC));
    } catch (IOException e) {
      removeMount(mountId);
      throw new UnavailableException(
          String.format("Failed to connect to UFS %s with id %d", info.getUri(), mountId), e);
    }
    return ufs;
  }
}
