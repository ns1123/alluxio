/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.adjust;

import alluxio.client.ClientContext;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockInStream;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.UnderStoreBlockInStream;
import alluxio.client.file.DirectUnderStoreStreamFactory;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.NoWorkerException;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.util.SerializableVoid;
import alluxio.master.block.BlockId;
import alluxio.util.IdUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job to replicate a block. This job is invoked by the checker of replication level in
 * FileSystemMaster.
 */
@NotThreadSafe
public final class ReplicateDefinition
    extends AbstractVoidJobDefinition<ReplicateConfig, SerializableVoid> {

  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final FileSystemContext mFileSystemContext;

  /**
   * Constructs a new {@link ReplicateDefinition} instance.
   */
  public ReplicateDefinition() {
    mFileSystemContext = FileSystemContext.INSTANCE;
  }

  /**
   * Constructs a new {@link ReplicateDefinition} instance with FileSystem context and instance.
   *
   * @param fileSystemContext file system context
   */
  public ReplicateDefinition(FileSystemContext fileSystemContext) {
    mFileSystemContext = fileSystemContext;
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
    int numReplicas = config.getReplicas();
    Preconditions.checkArgument(numReplicas > 0);

    AlluxioBlockStore blockStore = mFileSystemContext.getAlluxioBlockStore();
    BlockInfo blockInfo = blockStore.getInfo(blockId);

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
    AlluxioBlockStore blockStore = mFileSystemContext.getAlluxioBlockStore();

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
      throw new NoWorkerException(ExceptionMessage.NO_LOCAL_BLOCK_WORKER_REPLICATE_TASK
          .getMessage(blockId));
    }

    try (BlockInStream inputStream = createInputStream(blockId, blockStore);
         OutputStream outputStream = blockStore
             .getOutStream(blockId, -1, // use -1 to reuse the existing block size for this block
                 localNetAddress, OutStreamOptions.defaults())) {
      ByteStreams.copy(inputStream, outputStream);
    }
    return null;
  }

  /**
   * Creates an input stream for the given block. If the block is stored in Alluxio, the input
   * stream will read the worker having this block; otherwise, try to read from ufs.
   *
   * @param blockId block ID
   * @param blockStore handler to Alluxio block store
   * @return the input stream
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  private BlockInStream createInputStream(long blockId, AlluxioBlockStore blockStore)
      throws AlluxioException, IOException {
    BlockInfo blockInfo = blockStore.getInfo(blockId);
    // This block is stored in Alluxio, read it from Alluxio worker
    if (blockInfo.getLocations().size() > 0) {
      return blockStore.getInStream(blockId, InStreamOptions.defaults());
    }
    // Not stored in Alluxio, try to read it from UFS if its file is persisted
    FileInfo fileInfo;

    // the file id is the container id of the block id
    // TODO(binfan): this should be consolidated into a util function
    long containerId = BlockId.getContainerId(blockId);
    long fileId = IdUtils.createFileId(containerId);

    try (FileSystemMasterClient client =
             new FileSystemMasterClient(ClientContext.getMasterAddress())) {
      fileInfo = client.getFileInfo(fileId);
    }
    if (!fileInfo.isPersisted()) {
      throw new IOException("Block " + blockId + " is not found in Alluxio and ufs.");
    }

    long blockLength = blockInfo.getLength();
    String ufsPath = fileInfo.getUfsPath();
    long blockSize = fileInfo.getBlockSizeBytes();
    long blockStart = BlockId.getSequenceNumber(blockId) * blockSize;

    return new UnderStoreBlockInStream(blockStart, blockLength, blockSize,
        new DirectUnderStoreStreamFactory(ufsPath));
  }
}
