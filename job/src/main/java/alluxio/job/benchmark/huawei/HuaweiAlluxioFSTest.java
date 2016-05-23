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
 * Huawei POC test.
 */
public class HuaweiAlluxioFSTest {
  private final String path;
  private final int depth;
  private final int width;
  private final int count;
  private final int size;

  /**
   * Huawei test.
   * @param path the root path
   * @param depth the depth of the file system tree starting from path
   * @param width the with of the file system tree
   * @param count the number of files (i.e. leaves)
   * @param size the file size
   */
  public HuaweiAlluxioFSTest(String path, int depth, int width, int count, int size) {
    this.path = path;
    this.depth = depth;
    this.width = width;
    this.count = count;
    this.size = size;
  }

  /**
   * Build all paths give depth and parent path.
   *
   * @param parentPath the parent path
   * @param depth the depth of the file system tree starting from parent path
   * @return all the paths
   */
  private String[] buildPath(String parentPath, int depth) {
    String curPath[] = new String[this.width];

    for (int i = 1; i <= curPath.length; i++) {
      curPath[i - 1] = parentPath + "/vdb." + depth + "_" + i + ".dir";
    }

    return curPath;
  }

  /**
   * Do all the file operations (read or write).
   *
   * @param parentPath the parent path
   * @param operation the operation
   * @throws Exception if anything fails
   */
  private void recurseFile(String parentPath, FileOperation operation) throws Exception {
    for (int i = 1; i <= this.count; i++) {
      String filePath =
          parentPath + "/vdb_f" + String.format("%0" + String.valueOf(this.count).length() + "d", i)
              + ".file";

      AlluxioURI uri = new AlluxioURI(filePath);
      operation.run(uri);
    }
  }

  /**
   * Recursively operation on a tree node.
   *
   * @param parentPath the parent path
   * @param depth the depth
   * @param operation the operation
   * @throws Exception if anything fails
   */
  private void recursePath(String parentPath, int depth, FileOperation operation) throws Exception {
    String curPath[] = buildPath(parentPath, depth);

    if (depth == this.depth) {
      for (String path : curPath) {
        recurseFile(path, operation);
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
    recursePath(this.path, 1, new ReadFileOperation(this.size, type));
  }

  /**
   * Test writing files.
   *
   * @param type the write type
   * @throws Exception if file write fails
   */
  public void testWriteFile(WriteType type) throws Exception {
    recursePath(this.path, 1, new WriteFileOperation(this.size, type));
  }
}


