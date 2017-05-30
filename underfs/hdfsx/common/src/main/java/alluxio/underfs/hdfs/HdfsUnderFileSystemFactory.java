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

package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link HdfsUnderFileSystem}.
 *
 * It caches created {@link HdfsUnderFileSystem}s, using the scheme and authority pair as the key.
 */
@ThreadSafe
public class HdfsUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUnderFileSystemFactory.class);
  // ALLUXIO CS ADD
  private static UnderFileSystem createHdfsUfs(String path, UnderFileSystemConfiguration conf) {
    String versionStr = conf.getValue(PropertyKey.UNDERFS_HDFS_VERSION);
    HdfsVersion version = HdfsVersion.find(versionStr);
    if (version == null) {
      throw new RuntimeException("Unknown Hdfs version " + versionStr);
    }

    @SuppressWarnings("unchecked") Class<?> clazz;
    String hdfsUfsClassName = version.getHdfsUfsClassName();
    ClassLoader classLoader = version.getHdfsUfsClassLoader();
    try {
      clazz = classLoader.loadClass(hdfsUfsClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(String.format("Failed to load class %s", hdfsUfsClassName), e);
    }
    UnderFileSystem ufs;
    try {
      ufs = (UnderFileSystem) CommonUtils.createNewClassInstance(clazz,
          new Class[] {AlluxioURI.class, UnderFileSystemConfiguration.class},
          new Object[] {new AlluxioURI(path), conf});
    } catch (Exception e) {
      throw new RuntimeException(String
          .format("HdfsUnderFileSystem class for version %s could not be instantiated", version),
          e);
    }
    return ufs;
  }
  // ALLUXIO CS END

  /**
   * Constructs a new {@link HdfsUnderFileSystemFactory}.
   */
  public HdfsUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path);
    // ALLUXIO CS REPLACE
    // return HdfsUnderFileSystem.createInstance(new AlluxioURI(path), conf);
    // ALLUXIO CS WITH
    return createHdfsUfs(path, conf);
    // ALLUXIO CS END
  }

  @Override
  public boolean supportsPath(String path) {
    if (path != null) {
      // TODO(hy): In Hadoop 2.x this can be replaced with the simpler call to
      // FileSystem.getFileSystemClass() without any need for having users explicitly declare the
      // file system schemes to treat as being HDFS. However as long as pre 2.x versions of Hadoop
      // are supported this is not an option and we have to continue to use this method.
      for (final String prefix : Configuration.getList(PropertyKey.UNDERFS_HDFS_PREFIXES, ",")) {
        if (path.startsWith(prefix)) {
          return true;
        }
      }
    }
    return false;
  }
}
