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
import alluxio.client.file.policy.SpecificWorkerPolicy;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.master.block.BlockId;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.worker.WorkerContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple loading job that loads the blocks of a file in a distributed and round-robin fashion.
 */
@NotThreadSafe
public final class LoadDefinition extends AbstractVoidJobDefinition<LoadConfig, List<Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
  private static final int BUFFER_SIZE = 500 * Constants.MB;

  @Override
  public Map<WorkerInfo, List<Long>> selectExecutors(LoadConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    List<FileBlockInfo> blockInfoList = jobMasterContext.getFileSystem().listBlocks(uri);
    Map<WorkerInfo, List<Long>> result = Maps.newHashMap();

    int count = 0;
    for (FileBlockInfo blockInfo : blockInfoList) {
      if (!blockInfo.getBlockInfo().getLocations().isEmpty()) {
        continue;
      }
      // load into the next worker
      WorkerInfo workerInfo = jobWorkerInfoList.get(count);
      if (!result.containsKey(workerInfo)) {
        result.put(workerInfo, Lists.<Long>newArrayList());
      }
      List<Long> list = result.get(workerInfo);
      list.add(blockInfo.getBlockInfo().getBlockId());
      count = (count + 1) % jobWorkerInfoList.size();
    }

    return result;
  }

  @Override
  public Void runTask(LoadConfig config, List<Long> args, JobWorkerContext jobWorkerContext)
      throws Exception {
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    long blockSize = jobWorkerContext.getFileSystem().getStatus(uri).getBlockSizeBytes();
    byte[] buffer = new byte[BUFFER_SIZE];

    for (long blockId : args) {
      BlockInfo blockInfo = AlluxioBlockStore.get().getInfo(blockId);
      long length = blockInfo.getLength();
      long offset = blockSize * BlockId.getSequenceNumber(blockId);

      OpenFileOptions options = OpenFileOptions.defaults()
          .setLocationPolicy(new SpecificWorkerPolicy(WorkerContext.getNetAddress()));
      FileInStream inStream = jobWorkerContext.getFileSystem().openFile(uri, options);
      inStream.seek(offset);
      inStream.read(buffer, 0, BUFFER_SIZE);
      inStream.close();
      LOG.info("Loaded block:" + blockId + " with offset " + offset + " and length " + length
          + " into worker " + WorkerContext.getNetAddress());
    }

    return null;
  }
}
