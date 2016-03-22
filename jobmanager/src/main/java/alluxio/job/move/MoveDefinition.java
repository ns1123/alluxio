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
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.job.JobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.JobUtils;
import alluxio.master.file.FileSystemMaster;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import jersey.repackaged.com.google.common.base.Preconditions;
import jersey.repackaged.com.google.common.collect.Lists;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.ConcurrentMap;

/**
 * A job that moves a source file to a destination path.
 */
public final class MoveDefinition implements JobDefinition<MoveConfig, List<MoveOrder>> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final Random mRandom = new Random();

  /**
   * {@inheritDoc}
   *
   * Assign each worker to move whichever files it has the most blocks for.
   */
  @Override
  public Map<WorkerInfo, List<MoveOrder>> selectExecutors(MoveConfig config,
      List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Preconditions.checkState(workerInfoList.size() > 0);

    FileSystemMaster fileSystemMaster = jobMasterContext.getFileSystemMaster();
    AlluxioURI src = config.getSrc();

    // If the destination is already a folder, put the source file inside it.
    AlluxioURI dst = config.getDst();
    try {
      FileInfo info = fileSystemMaster.getFileInfo(config.getDst());
      if (info.isFolder()) {
        dst = new AlluxioURI(PathUtils.concatPath(dst, src.getName()));
      }
    } catch (FileDoesNotExistException e) {
      // Fall through, dst is set correctly.
    }

    List<FileInfo> files = getFilesToMove(config.getSrc(), fileSystemMaster);

    ConcurrentMap<WorkerInfo, List<MoveOrder>> assignments = Maps.newConcurrentMap();
    // Assign each file to the worker with the most block locality.
    for (FileInfo file : files) {
      AlluxioURI uri = new AlluxioURI(file.getPath());
      WorkerInfo bestWorker = JobUtils.getWorkerWithMostBlocks(workerInfoList,
          fileSystemMaster.getFileBlockInfoList(uri));
      if (bestWorker == null) {
        // Nobody has blocks, choose a random worker
        bestWorker = workerInfoList.get(mRandom.nextInt(workerInfoList.size()));
      }

      assignments.putIfAbsent(bestWorker, Lists.<MoveOrder>newArrayList());
      String relativePath = PathUtils.subtractPaths(file.getPath(), src.getPath());
      String dstPath = PathUtils.concatPath(dst, relativePath);
      assignments.get(bestWorker).add(new MoveOrder(file.getPath(), dstPath));
    }

    return assignments;
  }

  /**
   * @param src the path to move
   * @param jobMasterContext job master context
   * @return a list of the {@link FileInfo} for all files at the given path
   */
  private List<FileInfo> getFilesToMove(AlluxioURI src, FileSystemMaster fileSystemMaster)
      throws Exception {
    // Depth-first search to to find all files under src
    Stack<AlluxioURI> pathsToConsider = new Stack<>();
    pathsToConsider.add(src);
    List<FileInfo> files = Lists.newArrayList();
    while (!pathsToConsider.isEmpty()) {
      AlluxioURI path = pathsToConsider.pop();
      // If path is a file, the file info list will contain just the file info for that file.
      for (FileInfo info : fileSystemMaster.getFileInfoList(path)) {
        if (info.isFolder()) {
          pathsToConsider.push(new AlluxioURI(info.getPath()));
        } else {
          files.add(info);
        }
      }
    }
    return ImmutableList.copyOf(files);
  }

  /**
   * {@inheritDoc}
   *
   * Moves the file specified in the config to the configured path. If the destination path is a
   * directory, the file is moved inside that directory.
   */
  @Override
  public void runTask(MoveConfig config, List<MoveOrder> orders, JobWorkerContext jobWorkerContext)
      throws Exception {
    for (MoveOrder order : orders) {
      move(order.getSrc(), order.getDst(), jobWorkerContext.getFileSystem());
    }
  }

  /**
   * @param src the file to move
   * @param dst the path to move it to
   * @param fs the Alluxio file system
   */
  private void move(String src, String dst, FileSystem fs) throws Exception {
    LOG.debug("Moving {} to {}", src, dst);
    fs.createDirectory(new AlluxioURI(PathUtils.getParent(dst)),
        CreateDirectoryOptions.defaults().setAllowExists(true).setRecursive(true));
    try (FileInStream in = fs.openFile(new AlluxioURI(src));
        FileOutStream out = fs.createFile(new AlluxioURI(dst))) {
      IOUtils.copy(in, out);
      fs.delete(new AlluxioURI(src));
    }
  }
}
