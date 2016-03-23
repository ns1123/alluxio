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
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
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
    } catch (FileDoesNotExistException | InvalidPathException e) {
      // This is ok since dst is set correctly, but we should still check that the parent of dst
      // is a directory.
      if (!fileSystemMaster.getFileInfo(config.getDst().getParent()).isFolder()) {
        throw new FileDoesNotExistException(ExceptionMessage.MOVE_TO_FILE_AS_DIRECTORY
            .getMessage(config.getDst(), config.getDst().getParent()));
      }
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

      WriteType writeType =
          config.getWriteType() == null ? getWriteType(file) : config.getWriteType();
      assignments.get(bestWorker).add(new MoveOrder(file.getPath(), dstPath, writeType));
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
   * Returns the write type which makes the most sense for the given file info. If any part of the
   * file is cached and the file is also persisted, we count it as CACHE_THROUGH.
   *
   * @param fileInfo file information
   * @return the write type most likely used when writing the given fileInfo
   */
  private static WriteType getWriteType(FileInfo fileInfo) {
    if (fileInfo.isPersisted()) {
      if (fileInfo.getInMemoryPercentage() > 0) {
        return WriteType.CACHE_THROUGH;
      } else {
        return WriteType.THROUGH;
      }
    } else {
      return WriteType.MUST_CACHE;
    }
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
    FileSystem fs = jobWorkerContext.getFileSystem();
    for (MoveOrder order : orders) {
      move(order, fs);
    }
    // Try to delete the source directory if it is empty.
    if (!hasFiles(config.getSrc(), fs)) {
      try {
        LOG.info("Deleting {}", config.getSrc());
        fs.delete(config.getSrc(), DeleteOptions.defaults().setRecursive(true));
      } catch (FileDoesNotExistException e) {
        // It's already deleted, possibly by another worker.
      }
    }
    LOG.info("Done with all move orders");
  }

  /**
   * @param src the file to move
   * @param dst the path to move it to
   * @param fs the Alluxio file system
   * @param config move configuration
   */
  private static void move(MoveOrder order, FileSystem fs) throws Exception {
    String src = order.getSrc();
    String dst = order.getDst();
    LOG.info("Moving {} to {}", src, dst);
    fs.createDirectory(new AlluxioURI(PathUtils.getParent(dst)),
        CreateDirectoryOptions.defaults().setAllowExists(true).setRecursive(true));
    try (FileInStream in = fs.openFile(new AlluxioURI(src));
        FileOutStream out = fs.createFile(new AlluxioURI(dst),
            CreateFileOptions.defaults().setWriteType(order.getWriteType()))) {
      IOUtils.copy(in, out);
      fs.delete(new AlluxioURI(src));
    }
  }

  /**
   * @param src an Alluxio URI
   * @param fs the Alluxio file system
   * @return whether the URI is a file or a directory which contains files (including recursively)
   * @throws Exception if an unexpected exception occurs
   */
  private static boolean hasFiles(AlluxioURI src, FileSystem fs) throws Exception {
    Stack<AlluxioURI> dirsToCheck = new Stack<>();
    dirsToCheck.add(src);
    while (!dirsToCheck.isEmpty()) {
      try {
        for (URIStatus status : fs.listStatus(dirsToCheck.pop())) {
          if (!status.isFolder()) {
            return true;
          }
          dirsToCheck.push(new AlluxioURI(status.getPath()));
        }
      } catch (FileDoesNotExistException e) {
        // This probably means another worker has deleted the directory already, so we can probably
        // return false here. To be safe though, we will fall through and complete the search.
      }
    }
    return false;
  }
}
