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

public class FSMasterCreateDirDefinition extends AbstractThroughputLatencyJobDefinition {
  private FileSystemMasterClientPool mFileSystemMasterClientPool = null;

  @Override
  protected void before(ThroughputLatencyJobConfig config, JobWorkerContext jobWorkerContext)
    throws Exception {
    super.before(config, jobWorkerContext);
    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(ClientContext.getMasterAddress(), config.getThreadNum());
  }

  @Override
  protected boolean execute(ThroughputLatencyJobConfig config, JobWorkerContext jobWorkerContext,
      int commandId) {
    try {
      FileSystemMasterClient client = mFileSystemMasterClientPool.acquire();
      client.createDirectory(
          new AlluxioURI(getWorkDir(config, jobWorkerContext.getTaskId()) + "/" + commandId),
          CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true));
    } catch (AlluxioException e) {
      e.printStackTrace();
      return false;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }
}