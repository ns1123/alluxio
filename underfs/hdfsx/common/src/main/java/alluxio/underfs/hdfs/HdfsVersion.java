package alluxio.underfs.hdfs;

import alluxio.Configuration;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.util.io.PathUtils;

import java.net.URL;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/** The set of supported Hdfs versions. */
public enum HdfsVersion {
  APACHE_2_2("2.2", "(apache-)?2.2(.*)?", "apache2_2"), // Apache HDFS 2.2.*
  APACHE_2_7("2.7", "(apache-)?2.7(.*)?", "apache2_7"), // Apache HDFS 2.7.*
  ;

  // TODO(binfan): we may want to have a dedicated dir for the jars
  public static final String JAR_PATH_FORMAT =
      PathUtils.concatPath(//
          "file://" + Configuration.get(PropertyKey.HOME),
          "underfs/hdfsx/%s/target/alluxio-underfs-hdfsx-%s-%s.jar");
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
  public String getJarPath() {
    return String.format(JAR_PATH_FORMAT, mModuleName, mModuleName, ProjectConstants.VERSION);
  }

  /**
   * @return the corresponding class loader for this Hdfs version
   */
  public synchronized ClassLoader getHdfsUfsClassLoader() {
    if (mClassLoader != null) {
      return mClassLoader;
    }
    URL jarURL;
    try {
      jarURL = new URL(getJarPath());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mClassLoader = new IsolatedClassLoader(new URL[] {jarURL},
        new String[] {"org.apache.hadoop", // unshaded hadoop classes
            mHdfsUfsClassname, // HdfsUnderFileSystem for this version
            HdfsUnderFileSystem.class.getCanonicalName(), // superclass of HdfsUnderFileSystem
            "alluxio.underfs.hdfs.AtomicHdfsFileOutputStream", // creates FSDataOutputStream
            "alluxio.underfs.hdfs.HdfsUnderFileOutputStream", // creates FSDataOutputStream
            "alluxio.underfs.hdfs.HdfsUnderFileInputStream", // creates FSDataOutputStream
            "alluxio.underfs.hdfsx." + mModuleName // shaded classes of transitive dependencies
        },
        HdfsUnderFileSystemFactory.class.getClassLoader());
    return mClassLoader;
  }
}
