/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.adjust;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.NoWorkerException;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.SerializableVoid;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job to replicate a block. This job is invoked by the ReplicationChecker in FileSystemMaster.
 */
@NotThreadSafe
public final class ReplicateDefinition
    extends AbstractVoidJobDefinition<ReplicateConfig, SerializableVoid> {

  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final FileSystemContext mFileSystemContext;
  private final AlluxioBlockStore mAlluxioBlockStore;

  /**
   * Constructs a new {@link ReplicateDefinition}.
   */
  public ReplicateDefinition() {
    mFileSystemContext = FileSystemContext.INSTANCE;
    mAlluxioBlockStore = mFileSystemContext.getAlluxioBlockStore();
  }

  /**
   * Constructs a new {@link ReplicateDefinition} with FileSystem context and instance.
   *
   * @param fileSystemContext file system context
   * @param blockStore block store instance
   */
  public ReplicateDefinition(FileSystemContext fileSystemContext, AlluxioBlockStore blockStore) {
    mFileSystemContext = fileSystemContext;
    mAlluxioBlockStore = blockStore;
  }

  @Override
  public Class<ReplicateConfig> getJobConfigClass() {
    return ReplicateConfig.class;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(ReplicateConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Preconditions.checkArgument(!jobWorkerInfoList.isEmpty(), "No worker is available");

    long blockId = config.getBlockId();
    int numReplicas = config.getReplicateNumber();
    Preconditions.checkArgument(numReplicas != 0);

    BlockInfo blockInfo = mAlluxioBlockStore.getInfo(blockId);

    Set<String> hosts = new HashSet<>();
    for (BlockLocation blockLocation : blockInfo.getLocations()) {
      hosts.add(blockLocation.getWorkerAddress().getHost());
    }
    Map<WorkerInfo, SerializableVoid> result = Maps.newHashMap();

    Collections.shuffle(jobWorkerInfoList);
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      // Select job workers that don't have this block locally to adjust
      if (!hosts.contains(workerInfo.getAddress().getHost())) {
        result.put(workerInfo, null);
        if (result.size() >= numReplicas) {
          break;
        }
      }
    }
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * This task will replicate the block.
   */
  @Override
  public SerializableVoid runTask(ReplicateConfig config, SerializableVoid arg,
      JobWorkerContext jobWorkerContext) throws Exception {
    long blockId = config.getBlockId();

    String localHostName = NetworkAddressUtils.getLocalHostName();
    List<BlockWorkerInfo> workerInfoList = mAlluxioBlockStore.getWorkerInfoList();
    WorkerNetAddress localNetAddress = null;

    // TODO(bin): use LocalFirstPolicy here
    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getNetAddress().getHost().equals(localHostName)) {
        localNetAddress = workerInfo.getNetAddress();
        break;
      }
    }
    if (localNetAddress == null) {
      String message =
          String.format("Cannot find a local block worker to replicate block %d", blockId);
      LOG.error(message);
      throw new NoWorkerException(message);
    }

    try (InputStream inputStream = mAlluxioBlockStore.getInStream(blockId);
         OutputStream outputStream = mAlluxioBlockStore
             .getOutStream(blockId, -1 /* restoring an existing block */, localNetAddress)) {
      ByteStreams.copy(inputStream, outputStream);
    }
    return null;
  }

}
