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

import alluxio.Configuration;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.util.io.PathUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/** The set of supported Hdfs versions. */
public enum HdfsVersion {
  APACHE_2_2("2.2", "(apache-)?2.2(.*)?", "apache2_2"), // Apache HDFS 2.2.*
  APACHE_2_7("2.7", "(apache-)?2.7(.*)?", "apache2_7"), // Apache HDFS 2.7.*
  ;

  public static final String HDFS_JAR_FILENAME_FORMAT = "alluxio-underfs-hdfsx-%s-%s.jar";
  public static final String LIB_TEST_PATH_FORMAT = "file://" + PathUtils
      .concatPath(System.getProperty("user.dir"), "../lib/"); // for tests

  private final String mCanonicalVersion;
  private final Pattern mVersionPattern;
  private final String mModuleName;
  private final String mHdfsUfsClassname;
  private ClassLoader mClassLoader;

  /**
   * Constructs an instance of {@link HdfsVersion}.
   *
   * @param canonicalVersion the canonical version of an HDFS
   * @param versionPattern the regex pattern of version for an HDFS
   * @param moduleName the name of the module
   */
  HdfsVersion(String canonicalVersion, String versionPattern, String moduleName) {
    mModuleName = moduleName;
    mCanonicalVersion = canonicalVersion;
    mVersionPattern = Pattern.compile(versionPattern);
    mHdfsUfsClassname = String.format("alluxio.underfs.hdfs.%s.HdfsUnderFileSystem", moduleName);
    mClassLoader = null;
  }

  /**
   * @param versionString given version string
   * @return the corresponding {@link HdfsVersion} instance
   */
  @Nullable
  public static HdfsVersion find(String versionString) {
    for (HdfsVersion version : HdfsVersion.values()) {
      if (version.mVersionPattern.matcher(versionString).matches()) {
        return version;
      }
    }
    return null;
  }

  /**
   * @return the canonical version string
   */
  public String getCanonicalVersion() {
    return mCanonicalVersion;
  }

  /**
   * @return the module name
   */
  public String getModuleName() {
    return mModuleName;
  }

  /**
   * @return the class name of the HdfsUnderFileSystem implementation
   */
  public String getHdfsUfsClassName() {
    return mHdfsUfsClassname;
  }

  /**
   * @return the path to the jar of the UFS adaptor for this HDFS
   */
  public URL[] getJarPaths() {

    String jarFilename = String.format(HDFS_JAR_FILENAME_FORMAT, mModuleName, ProjectConstants.VERSION);
    try {
      URL libJarURL = new URL(
          "file://" + PathUtils.concatPath(Configuration.get(PropertyKey.LIB_DIR), jarFilename));
      URL libJarTestURL = new URL(
          PathUtils.concatPath(String.format(LIB_TEST_PATH_FORMAT, mModuleName), jarFilename));
      // NOTE, jars will be searched in the order that LIB_DIR first, then the path for test.
      return new URL[] {libJarURL, libJarTestURL};
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the corresponding class loader for this Hdfs version
   */
  public synchronized ClassLoader getHdfsUfsClassLoader() {
    if (mClassLoader != null) {
      return mClassLoader;
    }
    mClassLoader = new IsolatedClassLoader(getJarPaths(),
        new String[] {"org.apache.hadoop", // unshaded hadoop classes
            mHdfsUfsClassname, // HdfsUnderFileSystem for this version
            HdfsUnderFileSystem.class.getCanonicalName(), // superclass of HdfsUnderFileSystem
            "alluxio.underfs.hdfs.AtomicHdfsFileOutputStream", // creates FSDataOutputStream
            "alluxio.underfs.hdfs.HdfsUnderFileOutputStream", // creates FSDataOutputStream
            "alluxio.underfs.hdfs.HdfsUnderFileInputStream", // creates FSDataInputStream
            "alluxio.underfs.hdfsx." + mModuleName // shaded classes of transitive dependencies
        }, HdfsUnderFileSystemFactory.class.getClassLoader());
    return mClassLoader;
  }
}
