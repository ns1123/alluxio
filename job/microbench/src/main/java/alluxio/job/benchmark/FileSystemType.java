/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.job.fs.AbstractFS;
import alluxio.job.fs.AlluxioFS;
import alluxio.job.fs.HDFSFS;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Convenience modes for file system types, including ALLUXIO and HDFS for now. This can be broaden
 * to more supported benchmark file systems in the future.
 */
@ThreadSafe
public enum FileSystemType {
  /**
   * Alluxio file system.
   */
  ALLUXIO(1),

  /**
   * HDFS file system.
   */
  HDFS(2);

  private final int mValue;

  FileSystemType(int value) {
    mValue = value;
  }

  /**
   * @return the file system type value
   */
  public int getValue() {
    return mValue;
  }

  /**
   * Returns the file system abstract class based on the file system type.
   *
   * @return the abstract file system object based on the file system type, by default AlluxioFS
   */
  public AbstractFS getFileSystem() {
    if (mValue == HDFS.mValue) {
      return HDFSFS.get();
    }
    // default to AlluxioFS.
    return AlluxioFS.get();
  }

  /**
   * Returns the string format of the file system type.
   *
   * @returns the string format
   */
  @Override
  public String toString() {
    if (mValue == HDFS.mValue) {
      return "HDFS";
    }
    // default to AlluxioFS.
    return "ALLUXIO";
  }
}
