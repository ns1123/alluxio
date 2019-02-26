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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.meta.MountTable;
import alluxio.security.authorization.AuthorizationPluginConstants;
import alluxio.master.file.InodeAttributesProvider;
import alluxio.master.file.InodeAttributesProviderFactory;
import alluxio.underfs.UnderFileSystemConfiguration;

import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * An {@link InodeAttributesProvider} that allows Alluxio to retrieve inode attributes from
 * HDFS {@link INodeAttributeProvider}.
 */
public final class HdfsInodeAttributesProviderFactory implements InodeAttributesProviderFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsInodeAttributesProviderFactory.class);

  @Override
  public InodeAttributesProvider create(String path, @Nullable UnderFileSystemConfiguration conf,
      AlluxioConfiguration alluxioConf) {
    return new HdfsInodeAttributesProvider(conf);
  }

  @Override
  public boolean supportsPath(String path, @Nullable UnderFileSystemConfiguration conf) {
    boolean isHdfs = ServerConfiguration.getList(PropertyKey.UNDERFS_HDFS_PREFIXES, ",").stream()
        .anyMatch(prefix -> path.startsWith(prefix));
    if (conf != null && AuthorizationPluginConstants.AUTH_VERSION.equalsIgnoreCase(
        conf.get(PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME))
        && (isHdfs || path.equals(MountTable.ROOT))) {
      return true;
    }
    return false;
  }
}
