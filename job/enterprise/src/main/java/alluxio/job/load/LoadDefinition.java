/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.load;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.SpecificWorkerPolicy;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.load.LoadDefinition.LoadTask;
import alluxio.job.util.SerializableVoid;
import alluxio.job.util.SerializationUtils;
import alluxio.master.block.BlockId;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple loading job that loads the blocks of a file in a distributed and round-robin fashion.
 */
@NotThreadSafe
public final class LoadDefinition
    extends AbstractVoidJobDefinition<LoadConfig, ArrayList<LoadTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(LoadDefinition.class);
  private static final int MAX_BUFFER_SIZE = 500 * Constants.MB;

  private final FileSystem mFileSystem;

  /**
   * Constructs a new {@link LoadDefinition}.
   */
  public LoadDefinition() {
    mFileSystem = BaseFileSystem.get(FileSystemContext.INSTANCE);
  }

  /**
   * Constructs a new {@link LoadDefinition} with FileSystem context and instance.
   *
   * @param fileSystem file system client
   */
  public LoadDefinition(FileSystem fileSystem) {
    mFileSystem = fileSystem;
  }

  @Override
  public Map<WorkerInfo, ArrayList<LoadTask>> selectExecutors(LoadConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Map<String, Iterator<WorkerInfo>> jobWorkerIterators =
        createJobWorkerCyclicalIterators(jobWorkerInfoList);
    List<BlockWorkerInfo> blockWorkerInfoList = AlluxioBlockStore.create().getWorkerInfoList();
    List<BlockWorkerInfo> availableBlockWorkers = new ArrayList<>();
    for (BlockWorkerInfo blockWorkerInfo : blockWorkerInfoList) {
      if (jobWorkerIterators.containsKey(blockWorkerInfo.getNetAddress().getHost())) {
        availableBlockWorkers.add(blockWorkerInfo);
      }
    }
    // Mapping from worker to block ids which that worker is supposed to load.
    Multimap<WorkerInfo, LoadTask> blockAssignments = LinkedListMultimap.create();
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    List<FileBlockInfo> blockInfoList = mFileSystem.getStatus(uri).getFileBlockInfos();
    for (FileBlockInfo blockInfo : blockInfoList) {
      List<BlockWorkerInfo> blockWorkersWithoutBlock =
          getBlockWorkersWithoutBlock(availableBlockWorkers, blockInfo);
      int neededReplicas = config.getReplication() - blockInfo.getBlockInfo().getLocations().size();
      if (blockWorkersWithoutBlock.size() < neededReplicas) {
        throw new RuntimeException("Failed to find enough block workers to replicate to");
      }
      Collections.shuffle(blockWorkersWithoutBlock);
      for (int i = 0; i < neededReplicas; i++) {
        WorkerNetAddress blockWorkerAddress = blockWorkersWithoutBlock.get(i).getNetAddress();
        WorkerInfo jobWorker = jobWorkerIterators.get(blockWorkerAddress.getHost()).next();
        blockAssignments.put(jobWorker,
            new LoadTask(blockInfo.getBlockInfo().getBlockId(), blockWorkerAddress));
      }
    }
    return SerializationUtils.makeValuesSerializable(blockAssignments.asMap());
  }

  /**
   * @param blockWorkers a list of block workers
   * @param blockInfo information about a block
   * @return the block workers which are not storing the specified block
   */
  private List<BlockWorkerInfo> getBlockWorkersWithoutBlock(List<BlockWorkerInfo> blockWorkers,
      FileBlockInfo blockInfo) {
    Set<WorkerNetAddress> blockWorkerAddressesWithBlock = new HashSet<>();
    for (BlockLocation existingLocation : blockInfo.getBlockInfo().getLocations()) {
      blockWorkerAddressesWithBlock.add(existingLocation.getWorkerAddress());
    }
    List<BlockWorkerInfo> blockWorkersWithoutBlock = new ArrayList<BlockWorkerInfo>();
    for (BlockWorkerInfo blockWorker : blockWorkers) {
      if (!blockWorkerAddressesWithBlock.contains(blockWorker.getNetAddress())) {
        blockWorkersWithoutBlock.add(blockWorker);
      }
    }
    return blockWorkersWithoutBlock;
  }

  /**
   * @param jobWorkerInfoList a list of job worker information
   * @return a mapping from hostname to cyclical iterator over all job workers with that hostname
   */
  private Map<String, Iterator<WorkerInfo>> createJobWorkerCyclicalIterators(
      List<WorkerInfo> jobWorkerInfoList) {
    Multimap<String, WorkerInfo> indexedByHostname = ArrayListMultimap.create();
    for (WorkerInfo jobWorkerInfo : jobWorkerInfoList) {
      indexedByHostname.put(jobWorkerInfo.getAddress().getHost(), jobWorkerInfo);
    }
    Map<String, Iterator<WorkerInfo>> iterators = new HashMap<>();
    for (Entry<String, Collection<WorkerInfo>> entry : indexedByHostname.asMap().entrySet()) {
      iterators.put(entry.getKey(), Iterables.cycle(entry.getValue()).iterator());
    }
    return iterators;
  }

  @Override
  public SerializableVoid runTask(LoadConfig config, ArrayList<LoadTask> tasks,
      JobWorkerContext jobWorkerContext) throws Exception {
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    long blockSize = mFileSystem.getStatus(uri).getBlockSizeBytes();
    byte[] buffer = new byte[(int) Math.min(MAX_BUFFER_SIZE, blockSize)];

    for (LoadTask task : tasks) {
      long blockId = task.getBlockId();
      BlockInfo blockInfo = AlluxioBlockStore.create().getInfo(blockId);
      long length = blockInfo.getLength();
      long offset = blockSize * BlockId.getSequenceNumber(blockId);

      OpenFileOptions options = OpenFileOptions.defaults()
          .setLocationPolicy(new SpecificWorkerPolicy(task.getWorkerNetAddress()));
      FileInStream inStream = mFileSystem.openFile(uri, options);
      inStream.seek(offset);
      long bytesLeftToRead = length;
      while (bytesLeftToRead > 0) {
        int bytesRead = inStream.read(buffer, 0, (int) Math.min(bytesLeftToRead, buffer.length));
        if (bytesRead == -1) {
          break;
        }
        bytesLeftToRead -= bytesRead;
      }
      inStream.close();
      LOG.info("Loaded block " + blockId + " with offset " + offset + " and length " + length);
    }
    return null;
  }

  /**
   * A task representing loading a block into the memory of a worker.
   */
  public static class LoadTask implements Serializable {
    private static final long serialVersionUID = 2028545900913354425L;
    final long mBlockId;
    final WorkerNetAddress mWorkerNetAddress;

    /**
     * @param blockId the id of the block to load
     * @param workerNetAddress the address of the worker to load the block into
     */
    public LoadTask(long blockId, WorkerNetAddress workerNetAddress) {
      mBlockId = blockId;
      mWorkerNetAddress = workerNetAddress;
    }

    /**
     * @return the block id
     */
    public long getBlockId() {
      return mBlockId;
    }

    /**
     * @return the worker address
     */
    public WorkerNetAddress getWorkerNetAddress() {
      return mWorkerNetAddress;
    }
  }

  @Override
  public Class<LoadConfig> getJobConfigClass() {
    return LoadConfig.class;
  }
}
