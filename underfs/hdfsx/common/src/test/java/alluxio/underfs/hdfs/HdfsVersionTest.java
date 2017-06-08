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
  }

  @Test
  public void findByMavenArtifect() throws Exception {
    Assert.assertEquals(HdfsVersion.APACHE_1_0, HdfsVersion.find("1.0.0"));
    Assert.assertEquals(HdfsVersion.APACHE_1_2, HdfsVersion.find("1.2.0"));
    Assert.assertEquals(HdfsVersion.APACHE_2_2, HdfsVersion.find("2.2.0"));
    Assert.assertEquals(HdfsVersion.APACHE_2_3, HdfsVersion.find("2.3.0"));
    Assert.assertEquals(HdfsVersion.APACHE_2_4, HdfsVersion.find("2.4.0"));
    Assert.assertEquals(HdfsVersion.APACHE_2_5, HdfsVersion.find("2.5.0"));
    Assert.assertEquals(HdfsVersion.APACHE_2_6, HdfsVersion.find("2.6.0"));
    Assert.assertEquals(HdfsVersion.APACHE_2_7, HdfsVersion.find("2.7.0"));
    Assert.assertEquals(HdfsVersion.APACHE_2_8, HdfsVersion.find("2.8.0"));
    Assert.assertEquals(HdfsVersion.CDH_5_6, HdfsVersion.find("2.6.0-cdh5.6.0"));
    Assert.assertEquals(HdfsVersion.CDH_5_8, HdfsVersion.find("2.6.0-cdh5.8.0"));
    Assert.assertEquals(HdfsVersion.CDH_5_11, HdfsVersion.find("2.6.0-cdh5.11.0"));
    Assert.assertEquals(HdfsVersion.HDP_2_4, HdfsVersion.find("2.7.1.2.4.0.0-169"));
    Assert.assertEquals(HdfsVersion.HDP_2_5, HdfsVersion.find("2.7.3.2.5.0.0-1245"));
    Assert.assertEquals(HdfsVersion.MAPR_5_2, HdfsVersion.find("2.7.0-mapr-1607"));
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
  public void getHdfsUfsClassLoader() throws Exception {
    Assert.assertNotEquals(HdfsVersion.APACHE_2_2.getHdfsUfsClassLoader(),
        HdfsVersion.APACHE_2_7.getHdfsUfsClassLoader());
  }
}
