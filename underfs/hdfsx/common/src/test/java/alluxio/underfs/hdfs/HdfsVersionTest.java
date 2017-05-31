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
  public void getHdfsUfsClassLoader() throws Exception {
    Assert.assertNotEquals(HdfsVersion.APACHE_2_2.getHdfsUfsClassLoader(),
        HdfsVersion.APACHE_2_7.getHdfsUfsClassLoader());
  }
}
