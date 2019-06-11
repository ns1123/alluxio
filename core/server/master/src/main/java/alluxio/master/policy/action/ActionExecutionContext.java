/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.policy.action;

import alluxio.client.file.FileSystem;
import alluxio.client.job.JobMasterClientPool;
import alluxio.master.file.FileSystemMaster;

import java.util.concurrent.ExecutorService;

/**
 * Resources needed for ActionExecution.
 */
public final class ActionExecutionContext {
  private final FileSystemMaster mFileSystemMaster;
  private final FileSystem mFileSystem;
  private final ExecutorService mExecutorService;
  private final JobMasterClientPool mJobMasterClientPool;

  /**
   * Creates an instance of {@link ActionExecutionContext}.
   *
   * @param fileSystemMaster the filesystem master
   * @param executorService the executor service
   * @param jobMasterClientPool the job master client pool
   */
  public ActionExecutionContext(FileSystemMaster fileSystemMaster, ExecutorService executorService,
      JobMasterClientPool jobMasterClientPool) {
    mFileSystemMaster = fileSystemMaster;
    mFileSystem = FileSystem.Factory.get();
    mExecutorService = executorService;
    mJobMasterClientPool = jobMasterClientPool;
  }

  /**
   * @return the filesystem master
   */
  public FileSystemMaster getFileSystemMaster() {
    return mFileSystemMaster;
  }

  /**
   * @return a {@link FileSystem} client
   */
  public FileSystem getFileSystem() {
    return mFileSystem;
  }

  /**
   * @return the executor service to asynchronously execute actions
   */
  public ExecutorService getExecutorService() {
    return mExecutorService;
  }

  /**
   * @return the job master client pool to execute actions through job service
   */
  public JobMasterClientPool getJobMasterClientPool() {
    return mJobMasterClientPool;
  }
}
