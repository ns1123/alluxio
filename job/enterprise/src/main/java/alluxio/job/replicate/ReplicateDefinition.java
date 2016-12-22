/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.replicate;

import alluxio.AlluxioURI;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockInStream;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.UnderStoreBlockInStream;
import alluxio.client.file.DirectUnderStoreStreamFactory;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.NoWorkerException;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.SerializableVoid;
import alluxio.master.block.BlockId;
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

    AlluxioBlockStore blockStore = AlluxioBlockStore.create(mFileSystemContext);
    BlockInfo blockInfo = blockStore.getInfo(blockId);

    Set<String> hosts = new HashSet<>();
    for (BlockLocation blockLocation : blockInfo.getLocations()) {
      hosts.add(blockLocation.getWorkerAddress().getHost());
    }
    Map<WorkerInfo, SerializableVoid> result = Maps.newHashMap();

    Collections.shuffle(jobWorkerInfoList);
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      // Select job workers that don't have this block locally to replicate
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
    AlluxioBlockStore blockStore = AlluxioBlockStore.create(mFileSystemContext);

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

    try (BlockInStream inputStream = createInputStream(new AlluxioURI(config.getPath()), blockId,
        blockStore); OutputStream outputStream = blockStore
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
   * @param path the file path
   * @param blockId block ID
   * @param blockStore handler to Alluxio block store
   * @return the input stream
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  private BlockInStream createInputStream(AlluxioURI path, long blockId,
      AlluxioBlockStore blockStore) throws AlluxioException, IOException {
    BlockInfo blockInfo = blockStore.getInfo(blockId);
    // This block is stored in Alluxio, read it from Alluxio worker
    if (blockInfo.getLocations().size() > 0) {
      return blockStore.getInStream(blockId, InStreamOptions.defaults());
    }

    // TODO(jiri): Replace with internal client the uses file ID once the internal client is
    // factored out of the core server module.
    FileSystem fs = FileSystem.Factory.get();
    URIStatus status = fs.getStatus(path);
    if (!status.isPersisted()) {
      throw new IOException("Block " + blockId + " is not found in Alluxio and ufs.");
    }

    long blockLength = blockInfo.getLength();
    String ufsPath = status.getUfsPath();
    long blockSize = status.getBlockSizeBytes();
    long blockStart = BlockId.getSequenceNumber(blockId) * blockSize;

    return new UnderStoreBlockInStream(FileSystemContext.INSTANCE, blockStart, blockLength,
        blockSize, new DirectUnderStoreStreamFactory(ufsPath));
  }
}
