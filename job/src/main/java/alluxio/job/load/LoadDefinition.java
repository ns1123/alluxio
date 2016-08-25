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
import alluxio.client.file.FileInStream;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.master.block.BlockId;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
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
public final class LoadDefinition extends AbstractVoidJobDefinition<LoadConfig, Collection<Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
  private static final int BUFFER_SIZE = 500 * Constants.MB;

  /**
   * Constructs a new {@link LoadDefinition}.
   */
  public LoadDefinition() {}

  @Override
  public Map<WorkerInfo, Collection<Long>> selectExecutors(LoadConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    int replication = config.getReplication();
    Preconditions.checkState(replication <= jobWorkerInfoList.size(),
        "Replication cannot exceed the number of workers. Replication: %s, Num workers: %s",
        replication, jobWorkerInfoList.size());
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    List<FileBlockInfo> blockInfoList =
        jobMasterContext.getFileSystem().getStatus(uri).getFileBlockInfos();
    // Mapping from worker to block ids which that worker is supposed to load.
    Multimap<WorkerInfo, Long> blockAssignments = ArrayListMultimap.create();

    Iterator<WorkerInfo> workerIterator = Iterables.cycle(jobWorkerInfoList).iterator();
    for (FileBlockInfo blockInfo : blockInfoList) {
      Set<Long> workerIdsWithBlock = new HashSet<>();
      for (BlockLocation existingLocation : blockInfo.getBlockInfo().getLocations()) {
        workerIdsWithBlock.add(existingLocation.getWorkerId());
      }
      while (workerIdsWithBlock.size() < replication) {
        WorkerInfo workerInfo = workerIterator.next();
        if (!workerIdsWithBlock.contains(workerInfo.getId())) {
          blockAssignments.put(workerInfo, blockInfo.getBlockInfo().getBlockId());
          workerIdsWithBlock.add(workerInfo.getId());
        }
      }
    }

    return blockAssignments.asMap();
  }

  @Override
  public Void runTask(LoadConfig config, Collection<Long> args, JobWorkerContext jobWorkerContext)
      throws Exception {
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    long blockSize = jobWorkerContext.getFileSystem().getStatus(uri).getBlockSizeBytes();
    byte[] buffer = new byte[BUFFER_SIZE];

    for (long blockId : args) {
      BlockInfo blockInfo = AlluxioBlockStore.get().getInfo(blockId);
      long length = blockInfo.getLength();
      long offset = blockSize * BlockId.getSequenceNumber(blockId);

      OpenFileOptions options =
          OpenFileOptions.defaults().setLocationPolicy(new LocalFirstPolicy());
      FileInStream inStream = jobWorkerContext.getFileSystem().openFile(uri, options);
      inStream.seek(offset);
      inStream.read(buffer, 0, BUFFER_SIZE);
      inStream.close();
      LOG.info("Loaded block " + blockId + " with offset " + offset + " and length " + length);
    }

    return null;
  }
}
