/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job.benchmark;

import alluxio.AlluxioURI;
import alluxio.client.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.FileSystemMasterClientPool;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.exception.AlluxioException;
import alluxio.job.JobWorkerContext;

import jdk.nashorn.internal.scripts.JO;

import java.io.IOException;

public class FSMetaDefinition extends AbstractThroughputLatencyJobDefinition<FSMetaConfig> {
  private FileSystemMasterClientPool mFileSystemMasterClientPool = null;

  // mProducts is [dirSize^(level-1), dirSize^(level - 2), ... dirSize^0]. This is used to construct
  // path from an integer.
  private int[] mProducts;

  /**
   * Creates FSMasterCreateDirDefinition instance.
   */
  public FSMetaDefinition() {
  }

  @Override
  protected void before(FSMetaConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    super.before(config, jobWorkerContext);
    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(ClientContext.getMasterAddress(), config.getThreadNum());
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
   * Execute the command via {@link FileSystem}.
   *
   * @param config the create path config
   * @param jobWorkerContext the worker context
   * @param commandId the command Id
   * @return true if operation succeeds
   */
  private boolean executeFS(FSMetaConfig config, JobWorkerContext jobWorkerContext,
      int commandId) {
    FileSystem fileSystem = FileSystem.Factory.get();
    String path = constructPathFromCommandId(commandId);
    try {
      switch (config.getCommand()) {
        case CREATE_DIR:
          fileSystem.createDirectory(new AlluxioURI(path),
              CreateDirectoryOptions.defaults().setAllowExists(true).setRecursive(true));
          break;
        case CREATE_FILE:
          fileSystem
              .createFile(new AlluxioURI(path), CreateFileOptions.defaults().setRecursive(true))
              .close();
          break;
        case DELETE:
          fileSystem.delete(new AlluxioURI(path), DeleteOptions.defaults().setRecursive(true));
          break;
        case GET_STATUS:
          fileSystem.getStatus(new AlluxioURI(path));
          break;
        case LIST_STATUS:
          fileSystem.listStatus(new AlluxioURI(path), ListStatusOptions.defaults());
          break;
      }
    } catch (AlluxioException | IOException e) {
      LOG.warn("Path creation failed: ", e);
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
    FileSystemMasterClient client = mFileSystemMasterClientPool.acquire();
    try {
      String path = constructPathFromCommandId(commandId);
      switch (config.getCommand()) {
        case CREATE_DIR:
          client.createDirectory(new AlluxioURI(path),
              CreateDirectoryOptions.defaults().setAllowExists(true).setRecursive(true));
          break;
        case CREATE_FILE:
          client.createFile(new AlluxioURI(path), CreateFileOptions.defaults().setRecursive(true));
          client.completeFile(new AlluxioURI(path), CompleteFileOptions.defaults());
          break;
        case DELETE:
          client.delete(new AlluxioURI(path), DeleteOptions.defaults().setRecursive(true));
          break;
        case GET_STATUS:
          client.getStatus(new AlluxioURI(path));
          break;
        case LIST_STATUS:
          client.listStatus(new AlluxioURI(path), ListStatusOptions.defaults());
          break;
      }
    } catch (AlluxioException | IOException e) {
      LOG.warn("Path creation failed: ", e);
      return false;
    } finally {
      mFileSystemMasterClientPool.release(client);
    }
    return true;
  }

  /**
   * Constructs the file path to given a commandId.
   *
   * @param commandId the commandId. Each commandId corresponds to one file.
   * @return the file path to create
   */
  private String constructPathFromCommandId(int commandId) {
    StringBuilder path = new StringBuilder("/");
    for (int i = 0; i < mProducts.length; i++) {
      path.append(commandId / mProducts[i]);
      if (i != mProducts.length - 1) {
        path.append("/");
      }
      commandId %= mProducts[i];
    }
    return path.toString();
  }
}

