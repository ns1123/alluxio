/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.huawei;

import alluxio.AlluxioURI;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;

import java.io.IOException;

/**
 * The read file operation definition.
 */
class DeleteFileOperation implements FileOperation {
  /**
   * Creates {@link DeleteFileOperation} instance.
   */
  public DeleteFileOperation() {
  }

  @Override
  public void run(AlluxioURI uri) throws IOException, AlluxioException {
    try {
      FS.delete(uri, DeleteOptions.defaults().setRecursive(true));
    } catch (FileDoesNotExistException ex) {
      System.out.println("file " + uri.getPath() + " not exists: " + ex);
      throw ex;
    } catch (Exception ex) {
      System.out.println("open file " + uri.getPath() + " failed: " + ex);
      throw ex;
    }
  }
}

