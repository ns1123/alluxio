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
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.replicate.ReplicateDefinition.TaskType;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job to replicate a block.
 */
@NotThreadSafe
public final class ReplicateDefinition
    extends AbstractVoidJobDefinition<ReplicateConfig, TaskType> {

  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final FileSystemContext mFileSystemContext;
  private final BlockStoreContext mBlockStoreContext;

  /**
   * Constructs a new {@link ReplicateDefinition}.
   */
  public ReplicateDefinition() {
    mFileSystemContext = FileSystemContext.INSTANCE;
    mBlockStoreContext = mFileSystemContext.getBlockStoreContext();
  }

  @Override
  public Class<ReplicateConfig> getJobConfigClass() {
    return ReplicateConfig.class;
  }

  @Override
  public Map<WorkerInfo, TaskType> selectExecutors(ReplicateConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Preconditions.checkArgument(!jobWorkerInfoList.isEmpty(), "No worker is available");

    long blockId = config.getBlockId();
    int numReplicas = config.getNumReplicas();
    Preconditions.checkArgument(numReplicas != 0);

    AlluxioBlockStore blockStore = mFileSystemContext.getAlluxioBlockStore();
    BlockInfo blockInfo = blockStore.getInfo(blockId);

    Set<String> hosts = new HashSet<>();
    for (BlockLocation blockLocation : blockInfo.getLocations()) {
      hosts.add(blockLocation.getWorkerAddress().getHost());
    }
    Map<WorkerInfo, TaskType> result = Maps.newHashMap();

    boolean toReplicate = numReplicas > 0;
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      if (toReplicate) {
        // Select job workers that don't have this block locally to replicate
        if (!hosts.contains(workerInfo.getAddress().getHost())) {
          result.put(workerInfo, TaskType.REPLICATION);
        }
      } else {
        // Select job workers that have this block locally to evict
        if (hosts.contains(workerInfo.getAddress().getHost())) {
          result.put(workerInfo, TaskType.EVICTION);
        }
      }
      if (result.size() >= numReplicas) {
        break;
      }
    }

    return result;
  }

  /**
   * {@inheritDoc}
   *
   * Depending on the task type, this task will replicate the block if it is
   * {@link TaskType#REPLICATION}, and evict the given block if it is {@link TaskType#EVICTION}.
   */
  @Override
  public SerializableVoid runTask(ReplicateConfig config, TaskType taskType,
      JobWorkerContext jobWorkerContext) throws Exception {
    AlluxioBlockStore blockStore = mFileSystemContext.getAlluxioBlockStore();
    long blockId = config.getBlockId();

    String localHostName = NetworkAddressUtils.getLocalHostName();
    List<BlockWorkerInfo> workerInfoList = blockStore.getWorkerInfoList();
    WorkerNetAddress localAddress = null;

    // TODO(bin): use LocalFirstPolicy here
    for (BlockWorkerInfo workerInfo : workerInfoList) {
      if (localAddress == null || workerInfo.getNetAddress().getHost().equals(localHostName)) {
        localAddress = workerInfo.getNetAddress();
      }
    }

    if (taskType == TaskType.REPLICATION) {

      if (localAddress == null) {
        LOG.error("Cannot find a local block worker to replicate block {}", blockId);
        return null;
      }

      InputStream inputStream = blockStore.getInStream(blockId);
      OutputStream outputStream =
          blockStore.getOutStream(blockId, -1 /* restoring an existing block */, localAddress);
      ByteStreams.copy(inputStream, outputStream);
    } else {
      try (BlockWorkerClient client = mBlockStoreContext.acquireWorkerClient(localAddress)) {
        client.removeBlock(blockId);
      } catch (BlockDoesNotExistException e) {
        LOG.error("Failed to delete block {} on {}: not exist", blockId, localAddress);
      }
    }
    return null;
  }

  /**
   * Indicates whether this task for replication job is to replicate or evict a block.
   */
  public enum TaskType {
    REPLICATION,
    EVICTION
  }
}
