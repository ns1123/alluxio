/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.adjust;


import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.BlockDoesNotExistException;
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

/**
 * A job to either adjust or evict a block. This job is invoked by the replication monitor in
 * FileSystemMaster. Given the block ID and the number of replicas to add or evict, this job will
 * select corresponding job workers to spawn either adjust or evict tasks to work on this block.
 * Note that, this job is not idempotent.
 */
@NotThreadSafe
public final class EvictDefinition extends
    AbstractVoidJobDefinition<ReplicateConfig, SerializableVoid> {

  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final FileSystemContext mFileSystemContext;
  private final BlockStoreContext mBlockStoreContext;
  private final AlluxioBlockStore mAlluxioBlockStore;

  /**
   * Constructs a new {@link EvictDefinition}.
   */
  public EvictDefinition() {
    mFileSystemContext = FileSystemContext.INSTANCE;
    mBlockStoreContext = mFileSystemContext.getBlockStoreContext();
    mAlluxioBlockStore = mFileSystemContext.getAlluxioBlockStore();
  }

  /**
   * Constructs a new {@link EvictDefinition} with FileSystem context and instance.
   *
   * @param fileSystemContext file system context
   * @param blockStoreContext block store context
   * @param blockStore block store instance
   */
  public EvictDefinition(FileSystemContext fileSystemContext, BlockStoreContext blockStoreContext,
      AlluxioBlockStore blockStore) {
    mFileSystemContext = fileSystemContext;
    mBlockStoreContext = blockStoreContext;
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
    int numReplicas = config.getReplicaChange();
    Preconditions.checkArgument(numReplicas != 0, "Evict zero replica.");

    BlockInfo blockInfo = mAlluxioBlockStore.getInfo(blockId);

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
  public SerializableVoid runTask(ReplicateConfig config, SerializableVoid args,
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
      LOG.error("Cannot find a local block worker to replicate block {}", blockId);
      return null;
    }

    try (BlockWorkerClient client = mBlockStoreContext.acquireWorkerClient(localNetAddress)) {
      client.removeBlock(blockId);
    } catch (BlockDoesNotExistException e) {
      LOG.error("Failed to delete block {} on {}: not exist", blockId, localNetAddress, e);
    }
    return null;
  }
}
