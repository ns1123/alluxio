/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.proto.journal.File;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * This benchmark measures the filesystem (mostly FileSystemMaster) metadata performance. It
 * drives Alluxio at a specified rate (a.k.a. expectedThroughput) and then measures the
 * performance of each operation.
 */
public class FSMetaDefinition extends AbstractThroughputLatencyJobDefinition<FSMetaConfig> {
  // mProducts is [dirSize^(level-1), dirSize^(level - 2), ... dirSize^0]. This is used to construct
  // path from an integer.
  // It is initialized here to avoid check style failure.
  private int[] mProducts = new int[1];

  /**
   * Creates FSMasterCreateDirDefinition instance.
   */
  public FSMetaDefinition() {
  }

  @Override
  protected void before(FSMetaConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    super.before(config, jobWorkerContext);
    mProducts = new int[config.getLevel()];
    mProducts[config.getLevel() - 1] = 1;
    for (int i = config.getLevel() - 2; i >= 0; i--) {
      mProducts[i] = mProducts[i + 1] * config.getDirSize();
    }
  }

  @Override
  public boolean execute(FSMetaConfig config, JobWorkerContext jobWorkerContext,
      int commandId) {
    if (config.isUseFileSystemClient()) {
      return executeFS(config, jobWorkerContext, commandId);
    } else {
      return executeFSMaster(config, jobWorkerContext, commandId);
    }
  }

  /**
   * Executes the command via {@link FileSystem}.
   *
   * @param config the create path config
   * @param jobWorkerContext the worker context
   * @param commandId the command Id
   * @return true if operation succeeds
   */
  private boolean executeFS(FSMetaConfig config, JobWorkerContext jobWorkerContext,
      int commandId) {
    AbstractFS fileSystem = config.getFileSystemType().getFileSystem();
    String path = constructPathFromCommandId(config, jobWorkerContext.getTaskId(), commandId);
    try {
      switch (config.getCommand()) {
        case CREATE_DIR:
          fileSystem.createDirectory(path, config.getWriteType());
          break;
        case CREATE_FILE:
          if (config.getFileSize() <= 0) {
            fileSystem.createEmptyFile(path, config.getWriteType());
          } else {
            writeFile(fileSystem, config, path);
          }
          break;
        case DELETE:
          fileSystem.delete(path, true);
          break;
        case LIST_STATUS:
          fileSystem.listStatusAndIgnore(path);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported command.");
      }
    } catch (IOException e) {
      LOG.warn("Command failed: ", e);
      return false;
    }
    return true;
  }

  /**
   * Execute the command via {@link FileSystemMasterClient}.
   *
   * @param config the create path config
   * @param jobWorkerContext the worker context
   * @param commandId the command Id
   * @return true if operation succeeds
   */
  private boolean executeFSMaster(FSMetaConfig config, JobWorkerContext jobWorkerContext,
      int commandId) {
    Preconditions.checkState(false, "Unsupported for now");
    return true;
  }

  /**
   * Create a file with a given file size at path.
   *
   * @param fileSystem the file system
   * @param config the config
   * @param path the path
   * @throws IOException if it fails to write the file
   */
  private void writeFile(AbstractFS fileSystem, FSMetaConfig config, String path)
      throws IOException {
    OutputStream outputStream = fileSystem.create(path, config.getBlockSize(), config.getWriteType());

    // Use a fixed buffer size (4KB).
    final long defaultBufferSize = 4096L;
    byte[] content = new byte[(int) Math.min(config.getFileSize(), defaultBufferSize)];
    Arrays.fill(content, (byte) 'a');
    long remaining = config.getFileSize();
    while (remaining >= defaultBufferSize) {
      outputStream.write(content);
      remaining -= defaultBufferSize;
    }
    if (remaining > 0) {
      outputStream.write(content, 0, (int) remaining);
    }
    outputStream.close();
  }

  /**
   * Constructs the file path to given a commandId.
   *
   * @param config the configuration
   * @param taskId the task Id
   * @param commandId the commandId. Each commandId corresponds to one file
   * @return the file path to create
   */
  private String constructPathFromCommandId(FSMetaConfig config, int taskId, int commandId) {
    StringBuilder path = new StringBuilder(getWorkDir(config, taskId));
    path.append("/");
    int level = config.getLevel() - config.getLevelIgnored();
    for (int i = 0; i < level; i++) {
      path.append(commandId / mProducts[i]);
      if (i != level - 1) {
        path.append("/");
      }
      commandId %= mProducts[i];
    }
    return path.toString();
  }
}

