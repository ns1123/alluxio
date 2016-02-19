/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.jobmanager.job.prefetch;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileInStream;
import alluxio.jobmanager.job.JobDefinition;
import alluxio.jobmanager.job.JobMasterContext;
import alluxio.jobmanager.job.JobWorkerContext;
import alluxio.master.block.BlockId;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;

/**
 * A simple prefetching job that loads the blocks of a file distributedly and in a round-robin
 * fashion.
 */
public class DistributedPrefetchingDefinition
    implements JobDefinition<DistributedPrefetchingConfig, List<Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
  private static final int BUFFER_SIZE = 10 * Constants.MB;


  @Override
  public String getName() {
    return "DistributedPrefetching";
  }

  @Override
  public Map<WorkerInfo, List<Long>> selectExecutors(DistributedPrefetchingConfig config,
      List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) throws Exception {
    AlluxioURI uri = config.getFilePath();
    List<FileBlockInfo> blockInfoList =
        jobMasterContext.getFileSystemMaster().getFileBlockInfoList(uri);
    Map<WorkerInfo, List<Long>> result = Maps.newHashMap();

    int count = 0;
    LOG.info(blockInfoList.toString());
    for (FileBlockInfo blockInfo : blockInfoList) {
      if (!blockInfo.getBlockInfo().getLocations().isEmpty()) {
        continue;
      }
      // load into the next worker
      WorkerInfo workerInfo = workerInfoList.get(count);
      if (!result.containsKey(workerInfo)) {
        result.put(workerInfo, Lists.<Long>newArrayList());
      }
      List<Long> list = result.get(workerInfo);
      list.add(blockInfo.getBlockInfo().getBlockId());
      count = (count+1) % workerInfoList.size();
    }

    return result;
  }

  @Override
  public void runTask(DistributedPrefetchingConfig config, List<Long> args,
      JobWorkerContext jobWorkerContext) throws Exception {
    long blockSize =
        jobWorkerContext.getFileSystem().getStatus(config.getFilePath()).getBlockSizeBytes();
    byte[] buffer = new byte[BUFFER_SIZE];

    for (long blockId : args) {
      BlockInfo blockInfo = AlluxioBlockStore.get().getInfo(blockId);
      long length = blockInfo.getLength();
      long offset = blockSize * BlockId.getSequenceNumber(blockId);
      FileInStream inStream = jobWorkerContext.getFileSystem().openFile(config.getFilePath());
      inStream.seek(offset);
      read(inStream, length, buffer);
      LOG.info("Loaded block:" + blockId+" with offset "+offset+" and length "+length);
    }
  }

  private void read(FileInStream inStream, long length, byte[] buffer) throws IOException {
    for (int count = 0; count < length; count += BUFFER_SIZE) {
      inStream.read(buffer, count, BUFFER_SIZE);
    }
  }
}
