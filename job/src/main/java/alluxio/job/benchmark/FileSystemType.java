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
 * Convenience method for file system types, including Alluxio and HDFS for now. This can be broaden
 * to more supported benchmark file systems in the future.
 */
@ThreadSafe
public class FileSystemType {
  /**
   * Returns the file system abstract class based on the file system type.
   *
   * @param type the file system type in string, either "Alluxio" or "HDFS"
   * @return the abstract file system object based on the file system type, by default AlluxioFS
   */
  public static AbstractFS getFileSystem(String type) {
    if (type.equalsIgnoreCase("Alluxio")) {
      return AlluxioFS.get();
    }
    if (type.equalsIgnoreCase("HDFS")) {
      return HDFSFS.get();
    }
    // default to AlluxioFS.
    return AlluxioFS.get();
  }
}
