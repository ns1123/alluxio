/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.hdfs.apache2_2;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import javax.annotation.concurrent.ThreadSafe;

/**
 * HDFS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public final class HdfsUnderFileSystem extends alluxio.underfs.hdfs.HdfsUnderFileSystem {

  /**
   * Constructs a new HDFS {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   */
  public HdfsUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration conf) {
    super(uri, conf);
  }
}
