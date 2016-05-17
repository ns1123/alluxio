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
import alluxio.exception.AlluxioException;
import alluxio.job.JobWorkerContext;

import jdk.nashorn.internal.scripts.JO;

import java.io.IOException;

public class CreatePathDefinition
    extends AbstractThroughputLatencyJobDefinition<CreatePathConfig> {
  private FileSystemMasterClientPool mFileSystemMasterClientPool = null;

  private int[] mProducts;

  /**
   * Creates FSMasterCreateDirDefinition instance.
   */
  public CreatePathDefinition() {
  }

  @Override
  protected void before(CreatePathConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    super.before(config, jobWorkerContext);
    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(ClientContext.getMasterAddress(), config.getThreadNum());
    // Precompute this to save some CPU.
    mProducts = new int[config.getLevel()];
    mProducts[config.getLevel() - 1] = 1;
    for (int i = config.getLevel() - 2; i >= 0; i--) {
      mProducts[i] = mProducts[i + 1] * config.getDirSize();
    }
  }

  @Override
  public boolean execute(CreatePathConfig config, JobWorkerContext jobWorkerContext,
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
  private boolean executeFS(CreatePathConfig config, JobWorkerContext jobWorkerContext,
      int commandId) {
    FileSystem fileSystem = FileSystem.Factory.get();
    String path = constructPathFromCommandId(commandId);
    try {
      if (config.isDirectory()) {
        fileSystem.createDirectory(new AlluxioURI(path),
            CreateDirectoryOptions.defaults().setAllowExists(true).setRecursive(true));
      } else {
        fileSystem.createFile(new AlluxioURI(path), CreateFileOptions.defaults().setRecursive(true))
            .close();
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
  private boolean executeFSMaster(CreatePathConfig config,
      JobWorkerContext jobWorkerContext, int commandId) {
    FileSystemMasterClient client = mFileSystemMasterClientPool.acquire();
    try {
      String path = constructPathFromCommandId(commandId);
      if (config.isDirectory()) {
        client.createDirectory(new AlluxioURI(path),
            CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true));
      } else {
        client.createFile(new AlluxioURI(path), CreateFileOptions.defaults().setRecursive(true));
        client.completeFile(new AlluxioURI(path), CompleteFileOptions.defaults());
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
