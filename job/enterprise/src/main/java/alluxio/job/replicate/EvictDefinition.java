/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.replicate;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.BlockDoesNotExistException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job to evict a block. This job is invoked by the checker of replication level in
 * FileSystemMaster.
 */
@NotThreadSafe
public final class EvictDefinition
    extends AbstractVoidJobDefinition<EvictConfig, SerializableVoid> {

  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final FileSystemContext mFileSystemContext;

  /**
   * Constructs a new {@link EvictDefinition}.
   */
  public EvictDefinition() {
    mFileSystemContext = FileSystemContext.INSTANCE;
  }

  /**
   * Constructs a new {@link EvictDefinition} with FileSystem context and instance.
   *
   * @param fileSystemContext file system context
   */
  public EvictDefinition(FileSystemContext fileSystemContext) {
    mFileSystemContext = fileSystemContext;
  }

  @Override
  public Class<EvictConfig> getJobConfigClass() {
    return EvictConfig.class;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(EvictConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Preconditions.checkArgument(!jobWorkerInfoList.isEmpty(), "No worker is available");

    long blockId = config.getBlockId();
    int numReplicas = config.getReplicas();

    AlluxioBlockStore blockStore = mFileSystemContext.getAlluxioBlockStore();
    BlockInfo blockInfo = blockStore.getInfo(blockId);

    Set<String> hosts = new HashSet<>();
    for (BlockLocation blockLocation : blockInfo.getLocations()) {
      hosts.add(blockLocation.getWorkerAddress().getHost());
    }
    Map<WorkerInfo, SerializableVoid> result = Maps.newHashMap();

    Collections.shuffle(jobWorkerInfoList);
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      // Select job workers that have this block locally to evict
      if (hosts.contains(workerInfo.getAddress().getHost())) {
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
   * This task will evict the given block.
   */
  @Override
  public SerializableVoid runTask(EvictConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext) throws Exception {
    AlluxioBlockStore blockStore = mFileSystemContext.getAlluxioBlockStore();
    BlockStoreContext blockStoreContext = mFileSystemContext.getBlockStoreContext();

    long blockId = config.getBlockId();
    String localHostName = NetworkAddressUtils.getLocalHostName();
    List<BlockWorkerInfo> workerInfoList = blockStore.getWorkerInfoList();
    WorkerNetAddress localNetAddress = null;

    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (workerInfo.getNetAddress().getHost().equals(localHostName)) {
        localNetAddress = workerInfo.getNetAddress();
        break;
      }
    }
    if (localNetAddress == null) {
      String message = String.format("Cannot find a local block worker to evict block %d", blockId);
      throw new NoWorkerException(message);
    }

    try (BlockWorkerClient client = blockStoreContext.createWorkerClient(localNetAddress)) {
      client.removeBlock(blockId);
    } catch (BlockDoesNotExistException e) {
      // Instead of throwing this exception, we continue here because the block to evict does not
      // exist on this worker anyway.
      LOG.warn("Failed to delete block {} on {}: block does not exist", blockId, localNetAddress);
    }
    return null;
  }
}
