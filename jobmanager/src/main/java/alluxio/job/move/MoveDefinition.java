/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.move;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.FileDoesNotExistException;
import alluxio.job.JobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Maps;
import jersey.repackaged.com.google.common.base.Preconditions;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A job that moves a source file to a destination path.
 */
public final class MoveDefinition implements JobDefinition<MoveConfig, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  /**
   * {@inheritDoc}
   *
   * Chooses a random worker to perform a single-file move.
   */
  @Override
  public Map<WorkerInfo, Void> selectExecutors(MoveConfig config, List<WorkerInfo> workerInfoList,
      JobMasterContext jobMasterContext) throws Exception {
    Preconditions.checkState(workerInfoList.size() > 0);
    Map<WorkerInfo, Void> assignments = Maps.newHashMap();
    // Assign move to a random worker.
    WorkerInfo worker = workerInfoList.get(new Random().nextInt(workerInfoList.size()));
    assignments.put(worker, null);
    return assignments;
  }

  /**
   * {@inheritDoc}
   *
   * Moves the file specified in the config to the configured path. If the destination path is a
   * directory, the file is moved inside that directory.
   */
  @Override
  public void runTask(MoveConfig config, Void args, JobWorkerContext jobWorkerContext)
      throws Exception {
    FileSystem fs = jobWorkerContext.getFileSystem();
    AlluxioURI src = config.getSrc();
    AlluxioURI dst = config.getDst();
    // If the destination is already a folder, put the source file inside it.
    try {
      if (fs.getStatus(dst).isFolder()) {
        dst = new AlluxioURI(PathUtils.concatPath(dst, src.getName()));
      }
    } catch (FileDoesNotExistException e) {
      // Fall through, dst is set correctly.
    }

    URIStatus info = fs.getStatus(src);
    if (info.isFolder()) {
      throw new RuntimeException("The move task does not currently support moving directories");
    }
    FileInStream in = fs.openFile(src);
    FileOutStream out = fs.createFile(dst);
    IOUtils.copy(in, out);
    LOG.info("Copied {} to {}", src, dst);
  }
}
