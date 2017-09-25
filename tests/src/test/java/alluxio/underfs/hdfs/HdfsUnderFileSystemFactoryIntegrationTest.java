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

import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration Tests for HdfsUnderFileSystemFactory.
 */
@Ignore("https://tachyonnexus.atlassian.net/browse/AE-227")
public class HdfsUnderFileSystemFactoryIntegrationTest {

  @Test
  public void create() throws Exception {
    HdfsUnderFileSystemFactory factory = new HdfsUnderFileSystemFactory();
    UnderFileSystemConfiguration conf = UnderFileSystemConfiguration.defaults();

    conf.setUserSpecifiedConf(ImmutableMap.of(PropertyKey.UNDERFS_HDFS_VERSION.toString(),
        HdfsVersion.APACHE_2_2.getCanonicalVersion()));
    UnderFileSystem hdfsUfs22 = factory.create("hdfs://localhost:9000/", conf);
    Assert.assertEquals(HdfsVersion.APACHE_2_2.getHdfsUfsClassLoader(),
        hdfsUfs22.getClass().getClassLoader());
    Assert.assertNotEquals(UnderFileSystemFactory.class.getClassLoader(),
        hdfsUfs22.getClass().getClassLoader());

    conf.setUserSpecifiedConf(ImmutableMap.of(PropertyKey.UNDERFS_HDFS_VERSION.toString(),
        HdfsVersion.APACHE_2_6.getCanonicalVersion()));
    UnderFileSystem hdfsUfs26 = factory.create("hdfs://localhost:9000/", conf);
    Assert.assertEquals(HdfsVersion.APACHE_2_6.getHdfsUfsClassLoader(),
        hdfsUfs26.getClass().getClassLoader());
    Assert.assertNotEquals(UnderFileSystemFactory.class.getClassLoader(),
        hdfsUfs26.getClass().getClassLoader());

    Assert.assertNotEquals(hdfsUfs22.getClass().getClassLoader(),
        hdfsUfs26.getClass().getClassLoader());
  }
}
