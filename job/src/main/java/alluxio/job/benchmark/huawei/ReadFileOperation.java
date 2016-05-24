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
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;

import java.io.IOException;

/**
 * The read file operation definition.
 */
class ReadFileOperation implements FileOperation {
  private int mSize;
  private ReadType mType;

  /**
   * Creates {@link ReadFileOperation} instance.
   *
   * @param size the file size
   * @param type the read type
   */
  public ReadFileOperation(int size, ReadType type) {
    mSize = size;
    mType = type;
  }

  @Override
  public void run(AlluxioURI uri) throws IOException, AlluxioException {
    OpenFileOptions options = OpenFileOptions.defaults();
    options.setReadType(mType);

    FileInStream in = null;

    try {
      in = FS.openFile(uri, options);
    } catch (FileDoesNotExistException ex) {
      System.out.println("file " + uri.getPath() + " not exists: " + ex);
      throw ex;
    } catch (Exception ex) {
      System.out.println("open file " + uri.getPath() + " failed: " + ex);
      throw ex;
    }

    try {
      for (int i = 0; i < mSize / 8; i++) {
        in.read(DATA_BUFFER.array());
      }
    } catch (IOException ex) {
      System.out.println("read file " + uri.getPath() + " failed: " + ex);
      throw ex;
    } finally {
      in.close();
    }
  }
}

