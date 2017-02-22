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

package alluxio.underfs.hdfs.apache12;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link alluxio.underfs.hdfs.HdfsUnderFileSystem}.
 *
 * It caches created {@link alluxio.underfs.hdfs.HdfsUnderFileSystem}s, using the scheme and
 * authority pair as the key.
 */
@ThreadSafe
public class HdfsUnderFileSystemFactory extends alluxio.underfs.hdfs.HdfsUnderFileSystemFactory {
  /**
   * Constructs a new {@link HdfsUnderFileSystemFactory}.
   */
  public HdfsUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, Object conf) {
    Preconditions.checkNotNull(path);
    return new HdfsUnderFileSystem(new AlluxioURI(path), conf);
  }
}
