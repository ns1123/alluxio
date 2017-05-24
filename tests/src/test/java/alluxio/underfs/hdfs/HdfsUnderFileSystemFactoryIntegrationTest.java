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
import org.junit.Test;

/**
 * Integration Tests for HdfsUnderFileSystemFactory.
 */
public class HdfsUnderFileSystemFactoryIntegrationTest {

  @Test
  public void createApache22() throws Exception {
    HdfsUnderFileSystemFactory factory = new HdfsUnderFileSystemFactory();
    UnderFileSystemConfiguration conf = UnderFileSystemConfiguration.defaults();
    conf.setUserSpecifiedConf(ImmutableMap.of(PropertyKey.UNDERFS_HDFS_VERSION.toString(), "2.2"));
    UnderFileSystem ufs = factory.create("hdfs://localhost:9000/", conf);
    Assert.assertTrue(ufs instanceof alluxio.underfs.hdfs.apache2_2.HdfsUnderFileSystem);
    Assert.assertNotEquals(UnderFileSystemFactory.class.getClassLoader(),
        ufs.getClass().getClassLoader());
    conf.setUserSpecifiedConf(ImmutableMap.of(PropertyKey.UNDERFS_HDFS_VERSION.toString(), "2.2.0"));
    ufs = factory.create("hdfs://localhost:9000/", conf);
    Assert.assertTrue(ufs instanceof alluxio.underfs.hdfs.apache2_2.HdfsUnderFileSystem);
    Assert.assertNotEquals(UnderFileSystemFactory.class.getClassLoader(),
        ufs.getClass().getClassLoader());
  }
}
