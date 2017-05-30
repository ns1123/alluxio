package alluxio.underfs.hdfs;

import alluxio.ProjectConstants;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link HdfsVersion}.
 */
public class HdfsVersionTest {

  @Test
  public void find() throws Exception {
    Assert.assertNull(HdfsVersion.find("NotValid"));
    for (HdfsVersion version : HdfsVersion.values()) {
      Assert.assertEquals(version, HdfsVersion.find(version.getCanonicalVersion()));
    }
    Assert.assertEquals(HdfsVersion.APACHE_2_2, HdfsVersion.find("apache-2.2"));
    Assert.assertEquals(HdfsVersion.APACHE_2_2, HdfsVersion.find("apache-2.2.0"));
    Assert.assertEquals(HdfsVersion.APACHE_2_2, HdfsVersion.find("apache-2.2.1-SNAPSHOT"));
  }

  @Test
  public void getHdfsUfsClassName() throws Exception {
    for (HdfsVersion version : HdfsVersion.values()) {
      Assert.assertEquals(
          String.format("alluxio.underfs.hdfs.%s.HdfsUnderFileSystem", version.getModuleName()),
          version.getHdfsUfsClassName());
    }
  }

  @Test
  public void getJarPath() throws Exception {
    for (HdfsVersion version : HdfsVersion.values()) {
      Assert.assertEquals(String
          .format(HdfsVersion.JAR_PATH_FORMAT, version.getModuleName(), version.getModuleName(),
              ProjectConstants.VERSION), version.getJarPath());
    }
  }

  @Test
  public void getHdfsUfsClassLoader() throws Exception {
    Assert.assertNotEquals(HdfsVersion.APACHE_2_2.getHdfsUfsClassLoader(),
        HdfsVersion.APACHE_2_7.getHdfsUfsClassLoader());
  }
}