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
import alluxio.client.WriteType;

/**
 * Huawei POC test. This is directly copied from what Huawei is doing by adding some boilerplate
 * code. It writes or reads files in Alluxio sequentially.
 */
public class HuaweiAlluxioFSTest {
  private final String mPath;
  private final int mDepth;
  private final int mWidth;
  private final int mCount;
  private final int mSize;

  /**
   * Huawei test.
   * @param path the root mPath
   * @param depth the mDepth of the file system tree starting from mPath
   * @param width the with of the file system tree
   * @param count the number of files (i.e. leaves)
   * @param size the file mSize
   */
  public HuaweiAlluxioFSTest(String path, int depth, int width, int count, int size) {
    mPath = path;
    mDepth = depth;
    mWidth = width;
    mCount = count;
    mSize = size;
  }

  /**
   * Build all mPaths give mDepth and parent mPath.
   *
   * @param parentPath the parent mPath
   * @param mDepth the mDepth of the file system tree starting from parent mPath
   * @return all the mPaths
   */
  private String[] buildPath(String parentPath, int mDepth) {
    String[] curPath = new String[mWidth];

    for (int i = 1; i <= curPath.length; i++) {
      curPath[i - 1] = parentPath + "/vdb." + mDepth + "_" + i + ".dir";
    }

    return curPath;
  }

  /**
   * Do all the file operations (read or write).
   *
   * @param parentPath the parent mPath
   * @param operation the operation
   * @throws Exception if anything fails
   */
  private void recurseFile(String parentPath, FileOperation operation) throws Exception {
    for (int i = 1; i <= mCount; i++) {
      String filePath =
          parentPath + "/vdb_f" + String.format("%0" + String.valueOf(mCount).length() + "d", i)
              + ".file";

      AlluxioURI uri = new AlluxioURI(filePath);
      operation.run(uri);
    }
  }

  /**
   * Recursively operation on a tree node.
   *
   * @param parentPath the parent mPath
   * @param depth the mDepth
   * @param operation the operation
   * @throws Exception if anything fails
   */
  private void recursePath(String parentPath, int depth, FileOperation operation) throws Exception {
    String[] curPath = buildPath(parentPath, depth);

    if (depth == mDepth) {
      for (String mPath : curPath) {
        recurseFile(mPath, operation);
      }
    } else {
      for (String path : curPath) {
        recursePath(path, depth + 1, operation);
      }
    }
  }

  /**
   * Test reading files.
   *
   * @param type the read type
   * @throws Exception if file read fails
   */
  public void testReadFile(ReadType type) throws Exception {
    recursePath(mPath, 1, new ReadFileOperation(mSize, type));
  }

  /**
   * Test writing files.
   *
   * @param type the write type
   * @throws Exception if file write fails
   */
  public void testWriteFile(WriteType type) throws Exception {
    recursePath(mPath, 1, new WriteFileOperation(mSize, type));
  }

  /**
   * Test deleting a file.
   *
   * @throws Exception if the file deletion fails
   */
  public void testDeleteFile() throws Exception {
    recursePath(mPath, 1, new DeleteFileOperation());
  }
}

