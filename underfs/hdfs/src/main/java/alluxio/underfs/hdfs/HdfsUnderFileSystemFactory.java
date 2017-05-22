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
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

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
  private static final Map<String, URLClassLoader> LOADERS = new HashMap<>();

  /**
   * @param hdfsModule module name of HDFS
   * @return the path to the jar of the UFS adaptor for this HDFS
   */
  private static String getHdfsUfsJarPath(String hdfsModule) {
    // TODO(binfan): we may want to have a dedicated dir for the jars
    return String
        .format("file://%s/alluxio/underfs/hdfsx/%s/target/alluxio-underfs-hdfsx-%s-%s.jar",
            Configuration.get(PropertyKey.WORK_DIR), hdfsModule, hdfsModule,
            ProjectConstants.VERSION);
  }

  /**
   * @param hdfsModule module name of HDFS
   * @return the class name of the HdfsUnderFileSystem implementation
   */
  private static String getHdfsUfsClassName(String hdfsModule) {
    return String.format("alluxio.underfs.hdfs.%s.HdfsUnderFileSystem", hdfsModule);
  }

  /** The Map from a user-specified HDFS version string to its submodule name. */
  private static final Map<String, String> VERSION_TO_MODULE_MAP =
      new ImmutableMap.Builder<String, String>()
          .put("(apache-)?1.2(.*)?", "apache12") // Apache HDFS 1.2.*
          .put("(apache-)?2.2(.*)?", "apache22") // Apache HDFS 2.2.*
          .put("(apache-)?2.7(.*)?", "apache27") // Apache HDFS 2.7.*
          .build();

  private static UnderFileSystem createHdfsUfs(String path, UnderFileSystemConfiguration conf) {
    String version = conf.getValue(PropertyKey.UNDERFS_HDFS_VERSION);
    String module = VERSION_TO_MODULE_MAP.get(version);
    if (module == null) {
      throw new RuntimeException("Unknown Hdfs version " + version);
    }
    URLClassLoader classLoader;
    if (LOADERS.containsKey(module)) {
      classLoader = LOADERS.get(module);
    } else {
      URL[] urls;
      try {
        urls = new URL[] {new URL(getHdfsUfsJarPath(module))};
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      classLoader = new URLClassLoader(urls, HdfsUnderFileSystemFactory.class.getClassLoader());
      LOADERS.put(module, classLoader);
    }
    @SuppressWarnings("unchecked") Class<?> clazz;
    String hdfsUfsClassName = getHdfsUfsClassName(module);
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
          .format("HdfsUnderFileSystem class for given version %s could not be instantiated",
              version), e);
    }
    return ufs;
  }

  // ALLUXIO CS END
  /**
   * Constructs a new {@link HdfsUnderFileSystemFactory}.
   */
  public HdfsUnderFileSystemFactory() {
  }

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
