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
import alluxio.client.file.FileInStream;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.SpecificWorkerPolicy;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.load.LoadDefinition.LoadTask;
import alluxio.master.block.BlockId;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple loading job that loads the blocks of a file in a distributed and round-robin fashion.
 */
@NotThreadSafe
public final class LoadDefinition extends AbstractVoidJobDefinition<LoadConfig, Collection<LoadTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
  private static final int BUFFER_SIZE = 500 * Constants.MB;

  /**
   * Constructs a new {@link LoadDefinition}.
   */
  public LoadDefinition() {}

  @Override
  public Map<WorkerInfo, Collection<LoadTask>> selectExecutors(LoadConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    int replication = config.getReplication();
    List<BlockWorkerInfo> blockWorkerInfoList =
        jobMasterContext.getFileSystemContext().getAlluxioBlockStore().getWorkerInfoList();
    Preconditions.checkState(replication <= blockWorkerInfoList.size(),
        "Replication cannot exceed the number of block workers. Replication: %s, Num workers: %s",
        replication, blockWorkerInfoList.size());
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    List<FileBlockInfo> blockInfoList =
        jobMasterContext.getFileSystem().getStatus(uri).getFileBlockInfos();
    // Mapping from worker to block ids which that worker is supposed to load.
    Multimap<WorkerInfo, LoadTask> blockAssignments = ArrayListMultimap.create();
    Map<String, Iterator<BlockWorkerInfo>> blockWorkerIterators =
        createPerHostBlockWorkerIterators(blockWorkerInfoList);
    Iterator<WorkerInfo> jobWorkerIterator = Iterables.cycle(jobWorkerInfoList).iterator();
    for (FileBlockInfo blockInfo : blockInfoList) {
      Set<WorkerNetAddress> blockWorkersWithBlock = new HashSet<>();
      for (BlockLocation existingLocation : blockInfo.getBlockInfo().getLocations()) {
        blockWorkersWithBlock.add(existingLocation.getWorkerAddress());
      }
      int attempts = 0;
      while (blockWorkersWithBlock.size() < replication) {
        if (attempts >= replication * jobWorkerInfoList.size()) {
          throw new RuntimeException("Failed to find enough block workers to replicate to.");
        }
        attempts++;
        WorkerInfo jobWorkerInfo = jobWorkerIterator.next();
        String jobWorkerHost = jobWorkerInfo.getAddress().getHost();
        Iterator<BlockWorkerInfo> blockWorkerIterator = blockWorkerIterators.get(jobWorkerHost);
        if (blockWorkerIterator == null) {
          // For some reason this job worker has no local block workers, so skip it.
          LOG.warn("Couldn't find block worker on host {}. Block worker hostnames are {}",
              jobWorkerHost, blockWorkerIterators.keySet());
          continue;
        }
        BlockWorkerInfo blockWorkerInfo = blockWorkerIterator.next();
        WorkerNetAddress blockWorkerAddress = blockWorkerInfo.getNetAddress();
        if (!blockWorkersWithBlock.contains(blockWorkerAddress)) {
          blockAssignments.put(jobWorkerInfo,
              new LoadTask(blockInfo.getBlockInfo().getBlockId(), blockWorkerAddress));
          blockWorkersWithBlock.add(blockWorkerAddress);
        }
      }
    }
    // The default collections created by asMap are not Serializable.
    return Maps.transformValues(blockAssignments.asMap(),
        new Function<Collection<LoadTask>, Collection<LoadTask>>() {
          @Override
          public Collection<LoadTask> apply(Collection<LoadTask> input) {
            return ImmutableList.copyOf(input);
          }
        });
  }

  /**
   * @param alluxioWorkerInfoList a list of all Alluxio block workers
   * @return a mapping from hostname to cyclical iterator over all block workers with that hostname
   */
  private static Map<String, Iterator<BlockWorkerInfo>> createPerHostBlockWorkerIterators(
      List<BlockWorkerInfo> alluxioWorkerInfoList) {
    Multimap<String, BlockWorkerInfo> perHostBlockWorkers = ArrayListMultimap.create();
    for (BlockWorkerInfo blockWorkerInfo : alluxioWorkerInfoList) {
      perHostBlockWorkers.put(blockWorkerInfo.getNetAddress().getHost(), blockWorkerInfo);
    }
    Map<String, Iterator<BlockWorkerInfo>> perHostAlluxioBlockWorkerIterators = new HashMap<>();
    for (Map.Entry<String, Collection<BlockWorkerInfo>> entry : perHostBlockWorkers.asMap()
        .entrySet()) {
      Iterator<BlockWorkerInfo> iterator = Iterables.cycle(entry.getValue()).iterator();
      perHostAlluxioBlockWorkerIterators.put(entry.getKey(), iterator);
    }
    return perHostAlluxioBlockWorkerIterators;
  }

  @Override
  public Void runTask(LoadConfig config, Collection<LoadTask> tasks,
      JobWorkerContext jobWorkerContext) throws Exception {
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    long blockSize = jobWorkerContext.getFileSystem().getStatus(uri).getBlockSizeBytes();
    byte[] buffer = new byte[BUFFER_SIZE];

    for (LoadTask task : tasks) {
      long blockId = task.getBlockId();
      BlockInfo blockInfo = AlluxioBlockStore.get().getInfo(blockId);
      long length = blockInfo.getLength();
      long offset = blockSize * BlockId.getSequenceNumber(blockId);

      OpenFileOptions options = OpenFileOptions.defaults()
          .setLocationPolicy(new SpecificWorkerPolicy(task.getWorkerNetAddress()));
      FileInStream inStream = jobWorkerContext.getFileSystem().openFile(uri, options);
      inStream.seek(offset);
      inStream.read(buffer, 0, BUFFER_SIZE);
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
}
