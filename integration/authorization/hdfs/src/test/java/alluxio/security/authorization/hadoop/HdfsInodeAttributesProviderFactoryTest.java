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

package alluxio.security.authorization.hadoop;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.InodeAttributesProvider;
import alluxio.master.file.meta.MountTable;
import alluxio.security.authorization.AuthorizationPluginConstants;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

/**
 * Unit tests for the {@link HdfsInodeAttributesProviderFactory}.
 */
public class HdfsInodeAttributesProviderFactoryTest {

  @Test
  public void factory() {
    HdfsInodeAttributesProviderFactory factory =
        new HdfsInodeAttributesProviderFactory();
    UnderFileSystemConfiguration conf =
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global())
        .createMountSpecificConf(ImmutableMap.of(
            PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME.getName(),
            AuthorizationPluginConstants.AUTH_VERSION,
            DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
                DummyHdfsProvider.class.getName()));
    UnderFileSystemConfiguration invalidConf =
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global())
        .createMountSpecificConf(ImmutableMap.of(
            PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME.getName(),
            "invalid-1.0",
            DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
            DummyHdfsProvider.class.getName()));
    assertTrue(factory.supportsPath(MountTable.ROOT, conf));
    assertTrue(factory.supportsPath("hdfs://localhost/test/path", conf));
    assertFalse(factory.supportsPath("hdfs://localhost/test/path", invalidConf));
    assertFalse(factory.supportsPath("hdfs://localhost/test/path", null));
    assertFalse(factory.supportsPath("s3a://bucket/test/path", conf));
    InodeAttributesProvider provider = factory.create("hdfs://localhost/test/path", conf);
    assertNotNull(provider);
  }
}
