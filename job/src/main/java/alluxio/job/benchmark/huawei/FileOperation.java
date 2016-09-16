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
import alluxio.client.file.FileSystem;

import java.nio.ByteBuffer;

/**
 * File operation interface.
 */
interface FileOperation {
  int BUFFER_SIZE = 8192;
  ByteBuffer DATA_BUFFER = ByteBuffer.allocate(BUFFER_SIZE);
  FileSystem FS = FileSystem.Factory.get();

  /**
   * Run the file operation.
   *
   * @param uri the uri
   * @throws Exception if the operation fails
   */
  void run(AlluxioURI uri) throws Exception;
}

