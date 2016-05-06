/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.AlluxioURI;
import alluxio.client.ClientContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.FileSystemMasterClientPool;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.exception.AlluxioException;
import alluxio.job.JobWorkerContext;

import java.io.IOException;

/**
 * Measure the throughput and latency of creating directories in file system master.
 */
public class FSMasterCreateDirDefinition
    extends AbstractThroughputLatencyJobDefinition<FSMasterCreateDirConfig> {
  private FileSystemMasterClientPool mFileSystemMasterClientPool = null;

  @Override
  protected void before(FSMasterCreateDirConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    super.before(config, jobWorkerContext);
    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(ClientContext.getMasterAddress(), config.getThreadNum());
  }

  @Override
  protected boolean execute(FSMasterCreateDirConfig config, JobWorkerContext jobWorkerContext,
      int commandId) {

    FileSystemMasterClient client = mFileSystemMasterClientPool.acquire();
    try {
      client.createDirectory(
          new AlluxioURI(getWorkDir(config, jobWorkerContext.getTaskId()) + "/" + commandId),
          CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true));
    } catch (AlluxioException e) {
      e.printStackTrace();
      return false;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    } finally {
       mFileSystemMasterClientPool.release(client);
    }
    return true;
  }
}