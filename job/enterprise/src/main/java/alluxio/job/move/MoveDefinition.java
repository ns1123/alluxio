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
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.ConcurrentMap;

/**
 * A job that moves a source file to a destination path.
 */
public final class MoveDefinition
    extends AbstractVoidJobDefinition<MoveConfig, ArrayList<MoveCommand>> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final Random mRandom = new Random();

  /**
   * Constructs a new {@link MoveDefinition}.
   */
  public MoveDefinition() {}

  /**
   * {@inheritDoc}
   *
   * Assigns each worker to move whichever files it has the most blocks for. If the source and
   * destination are under the same mount point, no executors are needed and the metadata move
   * operation is performed.
   */
  @Override
  public Map<WorkerInfo, ArrayList<MoveCommand>> selectExecutors(MoveConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    FileSystem fileSystem = jobMasterContext.getFileSystem();
    AlluxioURI source = config.getSource();
    // Moving a path to itself or its parent is a no-op.
    if (config.getSource().equals(config.getDestination())
        || config.getSource().getParent().equals(config.getDestination())) {
      return Maps.newHashMap();
    }
    // The source cannot be a prefix of the destination - that would be moving a path inside itself.
    if (PathUtils.hasPrefix(config.getDestination().toString(), source.toString())) {
      throw new RuntimeException(ExceptionMessage.MOVE_CANNOT_BE_TO_SUBDIRECTORY.getMessage(source,
          config.getDestination()));
    }

    // If the destination is already a folder, put the source path inside it.
    AlluxioURI destination = config.getDestination();
    try {
      URIStatus destinationInfo = fileSystem.getStatus(config.getDestination());
      if (destinationInfo.isFolder()) {
        destination = new AlluxioURI(PathUtils.concatPath(destination, source.getName()));
        destinationInfo = fileSystem.getStatus(destination);
        if (destinationInfo.isFolder()) {
          if (config.isOverwrite()) {
            throw new RuntimeException(
                ExceptionMessage.MOVE_OVERWRITE_DIRECTORY.getMessage(destination));
          }
          throw new FileAlreadyExistsException(destination);
        }
      }
      // The destination is an existing file.
      if (config.isOverwrite()) {
        fileSystem.delete(destination);
      } else {
        throw new FileAlreadyExistsException(destination);
      }
    } catch (FileDoesNotExistException e) {
      // It is ok for the destination to not exist.
    }

    if (underSameMountPoint(source, config.getDestination(), fileSystem)) {
      fileSystem.rename(source, destination);
      return Maps.newHashMap();
    }
    List<BlockWorkerInfo> alluxioWorkerInfoList =
        jobMasterContext.getFileSystemContext().getAlluxioBlockStore().getWorkerInfoList();
    Preconditions.checkState(!jobWorkerInfoList.isEmpty(), "No worker is available");
    List<AlluxioURI> srcDirectories = Lists.newArrayList();
    List<URIStatus> statuses = getFilesToMove(source, fileSystem, srcDirectories);
    moveDirectories(srcDirectories, source.getPath(), destination.getPath(), fileSystem);
    ConcurrentMap<WorkerInfo, ArrayList<MoveCommand>> assignments = Maps.newConcurrentMap();
    ConcurrentMap<String, WorkerInfo> hostnameToWorker = Maps.newConcurrentMap();
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      hostnameToWorker.put(workerInfo.getAddress().getHost(), workerInfo);
    }
    List<String> keys = new ArrayList<>();
    keys.addAll(hostnameToWorker.keySet());
    // Assign each file to the worker with the most block locality.
    for (URIStatus status : statuses) {
      AlluxioURI uri = new AlluxioURI(status.getPath());
      BlockWorkerInfo bestWorker = JobUtils.getWorkerWithMostBlocks(alluxioWorkerInfoList,
          fileSystem.getStatus(uri).getFileBlockInfos());
      if (bestWorker == null) {
        // Nobody has blocks, choose a random worker.
        bestWorker = alluxioWorkerInfoList.get(mRandom.nextInt(jobWorkerInfoList.size()));
      }
      // Map the best Alluxio worker to a job worker.
      WorkerInfo worker = hostnameToWorker.get(bestWorker.getNetAddress().getHost());
      if (worker == null) {
        worker = hostnameToWorker.get(keys.get(new Random().nextInt(keys.size())));
      }
      assignments.putIfAbsent(worker, Lists.<MoveCommand>newArrayList());
      String destinationPath =
          computeTargetPath(status.getPath(), source.getPath(), destination.getPath());
      assignments.get(worker).add(new MoveCommand(status.getPath(), destinationPath));
    }
    return assignments;
  }

  /**
   * @param path1 an Alluxio URI
   * @param path2 an Alluxio URI
   * @param fileSystem the Alluxio file system
   * @return whether the two URIs are under the same mount point
   */
  private static boolean underSameMountPoint(AlluxioURI path1, AlluxioURI path2,
      FileSystem fileSystem) throws Exception {
    AlluxioURI mountPoint1 = getNearestMountPoint(path1, fileSystem);
    AlluxioURI mountPoint2 = getNearestMountPoint(path2, fileSystem);
    return Objects.equal(mountPoint1, mountPoint2);
  }

  private static AlluxioURI getNearestMountPoint(AlluxioURI uri, FileSystem fileSystem)
      throws Exception {
    AlluxioURI currUri = uri;
    while (currUri != null) {
      try {
        if (fileSystem.getStatus(currUri).isMountPoint()) {
          return currUri;
        }
      } catch (FileDoesNotExistException e) {
        // Fall through to keep looking for mount points in ancestors.
      }
      currUri = currUri.getParent();
    }
    return null;
  }

  /**
   * Computes the path that the given file should end up at when source is moved to destination.
   *
   * @param file a path to move which may be a descendent path of the source path, e.g. /src/file
   * @param source the base source path being moved, e.g. /src
   * @param destination the path to move to, e.g. /dst/src
   * @return the path which file should be moved to, e.g. /dst/src/file
   */
  private static String computeTargetPath(String file, String source, String destination)
      throws Exception {
    String relativePath = PathUtils.subtractPaths(file, source);
    return PathUtils.concatPath(destination, relativePath);
  }

  /**
   * @param directories the directories to move
   * @param source the base source path being moved
   * @param destination the destination path which the source is being moved to
   * @param fileSystem the Alluxio file system
   */
  private static void moveDirectories(List<AlluxioURI> directories, String source,
      String destination, FileSystem fileSystem) throws Exception {
    for (AlluxioURI directory : directories) {
      String newDir = computeTargetPath(directory.getPath(), source, destination);
      fileSystem.createDirectory(new AlluxioURI(newDir), CreateDirectoryOptions.defaults());
    }
  }

  /**
   * Returns {@link FileInfo} for all files under the source path.
   *
   * If source is a file, this is just the file. This method additionally appends all discovered
   * subdirectories of source in the order in which they are found.
   *
   * @param source the path to move
   * @param fileSystem the Alluxio file system
   * @param directoryAggregator a list for storing URIs for directories
   * @return a list of the {@link FileInfo} for all files at the given path
   * @throws IOException if a non-Alluxio exception occurs
   * @throws FileDoesNotExistException if the given path does not exist
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   */
  private static List<URIStatus> getFilesToMove(AlluxioURI source, FileSystem fileSystem,
      List<AlluxioURI> directoryAggregator)
      throws FileDoesNotExistException, AlluxioException, IOException {
    // Depth-first search to to find all files under source.
    Stack<AlluxioURI> pathsToConsider = new Stack<>();
    pathsToConsider.add(source);
    List<URIStatus> files = Lists.newArrayList();
    while (!pathsToConsider.isEmpty()) {
      AlluxioURI path = pathsToConsider.pop();
      List<URIStatus> statuses = fileSystem.listStatus(path);
      // If path is a file, the file info list will contain just the file info for that file.
      if (!(statuses.size() == 1 && statuses.get(0).getPath().equals(source.getPath()))) {
        directoryAggregator.add(path);
      }
      for (URIStatus status : statuses) {
        if (status.isFolder()) {
          pathsToConsider.push(new AlluxioURI(status.getPath()));
        } else {
          files.add(status);
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
  public SerializableVoid runTask(MoveConfig config, ArrayList<MoveCommand> commands,
      JobWorkerContext jobWorkerContext) throws Exception {
    FileSystem fs = jobWorkerContext.getFileSystem();
    for (MoveCommand command : commands) {
      move(command, config.getWriteType(), fs);
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
    return null;
  }

  /**
   * @param command the move command to execute
   * @param writeType the write type to use for the moved file
   * @param fileSystem the Alluxio file system
   */
  private static void move(MoveCommand command, WriteType writeType, FileSystem fileSystem)
      throws Exception {
    String source = command.getSource();
    String destination = command.getDestination();
    LOG.debug("Moving {} to {}", source, destination);
    try (FileInStream in = fileSystem.openFile(new AlluxioURI(source));
        FileOutStream out = fileSystem.createFile(new AlluxioURI(destination),
            CreateFileOptions.defaults().setWriteType(writeType))) {
      IOUtils.copy(in, out);
      fileSystem.delete(new AlluxioURI(source));
    }
  }

  /**
   * @param source an Alluxio URI
   * @param fileSystem the Alluxio file system
   * @return whether the URI is a file or a directory which contains files (including recursively)
   * @throws Exception if an unexpected exception occurs
   */
  private static boolean hasFiles(AlluxioURI source, FileSystem fileSystem) throws Exception {
    Stack<AlluxioURI> dirsToCheck = new Stack<>();
    dirsToCheck.add(source);
    while (!dirsToCheck.isEmpty()) {
      try {
        for (URIStatus status : fileSystem.listStatus(dirsToCheck.pop())) {
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

  @Override
  public Class<MoveConfig> getJobConfigClass() {
    return MoveConfig.class;
  }
}
