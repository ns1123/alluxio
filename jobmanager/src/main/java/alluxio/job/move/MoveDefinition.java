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
import alluxio.exception.AccessControlException;
import alluxio.exception.FileAlreadyExistsException;
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
public final class MoveDefinition implements JobDefinition<MoveConfig, List<MoveCommand>> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final Random mRandom = new Random();

  /**
   * {@inheritDoc}
   *
   * Assigns each worker to move whichever files it has the most blocks for.
   */
  @Override
  public Map<WorkerInfo, List<MoveCommand>> selectExecutors(MoveConfig config,
      List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Preconditions.checkState(!workerInfoList.isEmpty(), "No worker is available");

    FileSystemMaster fileSystemMaster = jobMasterContext.getFileSystemMaster();
    AlluxioURI source = config.getSource();

    // If the destination is already a folder, put the source path inside it.
    AlluxioURI destination = config.getDestination();
    try {
      FileInfo info = fileSystemMaster.getFileInfo(config.getDestination());
      if (info.isFolder()) {
        destination = new AlluxioURI(PathUtils.concatPath(destination, source.getName()));
      } else {
        // The destination is an existing file.
        if (config.isOverwrite()) {
          fileSystemMaster.deleteFile(destination, false);
        } else {
          throw new FileAlreadyExistsException(destination);
        }
      }
    } catch (FileDoesNotExistException e) {
      // It is ok for the destination to not exist.
    }

    List<AlluxioURI> emptyDirectories = Lists.newArrayList();
    List<FileInfo> files = getFilesToMove(source, fileSystemMaster, emptyDirectories);
    moveDirectories(emptyDirectories, source.getPath(), destination.getPath(), fileSystemMaster);
    ConcurrentMap<WorkerInfo, List<MoveCommand>> assignments = Maps.newConcurrentMap();
    // Assign each file to the worker with the most block locality.
    for (FileInfo file : files) {
      AlluxioURI uri = new AlluxioURI(file.getPath());
      WorkerInfo bestWorker = JobUtils.getWorkerWithMostBlocks(workerInfoList,
          fileSystemMaster.getFileBlockInfoList(uri));
      if (bestWorker == null) {
        // Nobody has blocks, choose a random worker
        bestWorker = workerInfoList.get(mRandom.nextInt(workerInfoList.size()));
      }
      assignments.putIfAbsent(bestWorker, Lists.<MoveCommand>newArrayList());
      String destinationPath =
          computeMovedPath(file.getPath(), source.getPath(), destination.getPath());
      WriteType writeType =
          config.getWriteType() == null ? getWriteType(file) : config.getWriteType();
      assignments.get(bestWorker).add(new MoveCommand(file.getPath(), destinationPath, writeType));
    }
    return assignments;
  }

  /**
   * @param file a path to move which may be a descendent path of the source path, e.g. /src/file
   * @param source the base source path being moved, e.g. /src
   * @param destination the path to move to, e.g. /dst/src
   * @return the path which file should be moved to, e.g. /dst/src/file
   */
  private static String computeMovedPath(String file, String source, String destination)
      throws Exception {
    String relativePath = PathUtils.subtractPaths(file, source);
    return PathUtils.concatPath(destination, relativePath);
  }

  /**
   * @param directories the directories to move
   * @param source the base source path being moved
   * @param destination the destination path which the source is being moved to
   * @param fileSystemMaster Alluxio file system master
   */
  private static void moveDirectories(List<AlluxioURI> directories, String source, String destination,
      FileSystemMaster fileSystemMaster) throws Exception {
    for (AlluxioURI directory : directories) {
      String newDir = computeMovedPath(directory.getPath(), source, destination);
      fileSystemMaster.mkdir(new AlluxioURI(newDir),
          alluxio.master.file.options.CreateDirectoryOptions.defaults());
    }
  }

  /**
   * Returns {@link FileInfo} for all files under the source path.
   *
   * If source is a file, this is just the file. This method additionally stores any discovered
   * empty subdirectories of source in the given list.
   *
   * @param source the path to move
   * @param jobMasterContext job master context
   * @param emptyDirectoryAggregator a list for storing URIs for empty directories
   * @return a list of the {@link FileInfo} for all files at the given path
   * @throws FileDoesNotExistException if the source file does not exist
   * @throws InvalidPathException if the source URI is malformed
   * @throws AccessControlException if permission checking fails
   */
  private static List<FileInfo> getFilesToMove(AlluxioURI source, FileSystemMaster fileSystemMaster,
      List<AlluxioURI> emptyDirectoryAggregator)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    // Depth-first search to to find all files under source.
    Stack<AlluxioURI> pathsToConsider = new Stack<>();
    pathsToConsider.add(source);
    List<FileInfo> files = Lists.newArrayList();
    while (!pathsToConsider.isEmpty()) {
      AlluxioURI path = pathsToConsider.pop();
      // If path is a file, the file info list will contain just the file info for that file.
      List<FileInfo> listing = fileSystemMaster.getFileInfoList(path);
      if (listing.isEmpty()) {
        emptyDirectoryAggregator.add(path);
      }
      for (FileInfo info : listing) {
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
  public void runTask(MoveConfig config, List<MoveCommand> commands,
      JobWorkerContext jobWorkerContext) throws Exception {
    FileSystem fs = jobWorkerContext.getFileSystem();
    for (MoveCommand command : commands) {
      move(command, fs);
    }
    // Try to delete the source directory if it is empty.
    if (!hasFiles(config.getSource(), fs)) {
      try {
        LOG.debug("Deleting {}", config.getSource());
        fs.delete(config.getSource(), DeleteOptions.defaults().setRecursive(true));
      } catch (FileDoesNotExistException e) {
        // It's already deleted, possibly by another worker.
      }
    }
  }

  /**
   * @param command the move command to execute
   * @param fs the Alluxio file system
   * @param config move configuration
   */
  private static void move(MoveCommand command, FileSystem fs) throws Exception {
    String source = command.getSource();
    String destination = command.getDestination();
    LOG.debug("Moving {} to {}", source, destination);
    fs.createDirectory(new AlluxioURI(PathUtils.getParent(destination)),
        CreateDirectoryOptions.defaults().setAllowExists(true).setRecursive(true));
    try (FileInStream in = fs.openFile(new AlluxioURI(source));
        FileOutStream out = fs.createFile(new AlluxioURI(destination),
            CreateFileOptions.defaults().setWriteType(command.getWriteType()))) {
      IOUtils.copy(in, out);
      fs.delete(new AlluxioURI(source));
    }
  }

  /**
   * @param source an Alluxio URI
   * @param fs the Alluxio file system
   * @return whether the URI is a file or a directory which contains files (including recursively)
   * @throws Exception if an unexpected exception occurs
   */
  private static boolean hasFiles(AlluxioURI source, FileSystem fs) throws Exception {
    Stack<AlluxioURI> dirsToCheck = new Stack<>();
    dirsToCheck.add(source);
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
