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
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;

import java.io.IOException;

/**
 * Write file operation definition.
 */
class WriteFileOperation implements FileOperation {
  private int mSize;
  private WriteType mType;

  /**
   * Creates a {@link WriteFileOperation} instance.
   *
   * @param size the file size
   * @param type the write type
   */
  public WriteFileOperation(int size, WriteType type) {
    mSize = size;
    mType = type;
  }

  @Override
  public void run(AlluxioURI uri) throws IOException, AlluxioException {
    CreateFileOptions options = CreateFileOptions.defaults();
    options.setWriteType(mType);
    options.setRecursive(true);

    FileOutStream out = null;

    try {
      out = FS.createFile(uri, options);
    } catch (FileAlreadyExistsException ex) {
      System.out.println("file " + uri.getPath() + " already exists: " + ex);
      throw ex;
    } catch (Exception ex) {
      System.out.println("create file " + uri.getPath() + " failed: " + ex);
      throw ex;
    }

    try {
      for (int i = 0; i < mSize / 8; i++) {
        out.write(DATA_BUFFER.array());
      }
    } catch (IOException ex) {
      System.out.println("write file " + uri.getPath() + " failed: " + ex);
      throw ex;
    } finally {
      out.close();
    }
  }
}

