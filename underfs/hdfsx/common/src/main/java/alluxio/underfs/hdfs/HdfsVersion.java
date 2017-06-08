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
  APACHE_1_0("1.0", "1\\.0(\\.(\\d+))?", "apache1_0"),
  APACHE_1_2("1.2", "1\\.2(\\.(\\d+))?", "apache1_2"),
  APACHE_2_2("2.2", "2\\.2(\\.(\\d+))?", "apache2_2"),
  APACHE_2_3("2.3", "2\\.3(\\.(\\d+))?", "apache2_3"),
  APACHE_2_4("2.4", "2\\.4(\\.(\\d+))?", "apache2_4"),
  APACHE_2_5("2.5", "2\\.5(\\.(\\d+))?", "apache2_5"),
  APACHE_2_6("2.6", "2\\.6(\\.(\\d+))?", "apache2_6"),
  APACHE_2_7("2.7", "2\\.7(\\.(\\d+))?", "apache2_7"),
  APACHE_2_8("2.8", "2\\.8(\\.(\\d+))?", "apache2_8"),
  CDH_5_6("cdh5.6", "(cdh5\\.6(\\.(\\d+))?|2\\.6\\.0-cdh5\\.6\\.(.*)?)", "cdh5_6"),
  CDH_5_8("cdh5.8", "(cdh5\\.8(\\.(\\d+))?|2\\.6\\.0-cdh5\\.8\\.(.*)?)", "cdh5_8"),
  CDH_5_11("cdh5.11", "(cdh5\\.11(\\.(\\d+))?|2\\.6\\.0-cdh5\\.11\\.(.*)?)", "cdh5_11"),
  HDP_2_4("hdp2.4", "(hdp2\\.4(\\.(\\d+))?|2\\.7\\.1\\.2\\.4\\.(\\d+)\\.(\\d+)-(.*)?)", "hdp2_4"),
  HDP_2_5("hdp2.5", "(hdp2\\.5(\\.(\\d+))?|2\\.7\\.3\\.2\\.5\\.(\\d+)\\.(\\d+)-(.*)?)", "hdp2_5"),
  MAPR_5_2("mapr5.2", "(mapr5\\.2(\\.(\\d+))?|2\\.7\\.0-mapr-1607)", "mapr5_2"),
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
            "alluxio.underfs.hdfs.HdfsSecurityUtils", // util methods calls Hadoop classes
            "alluxio.underfs.hdfsx." + mModuleName // shaded classes of transitive dependencies
        }, HdfsUnderFileSystemFactory.class.getClassLoader());
    return mClassLoader;
  }
}
