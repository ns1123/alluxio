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

package alluxio.underfs.fork;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link ForkUnderFileSystem}.
 *
 * The URI used for creating a new instance of {@link ForkUnderFileSystem} is expected to use
 * the authority to specify an "alias" for the mount point. This makes it possible for Alluxio
 * to distinguish between different instances of {@link ForkUnderFileSystem} mounted to Alluxio.
 */
@ThreadSafe
public class ForkUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ForkUnderFileSystemFactory.class);

  /**
   * Constructs a new {@link ForkUnderFileSystemFactory}.
   */
  public ForkUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration ufsConf) {
    Preconditions.checkArgument(path != null, "path may not be null");
    AlluxioURI uri = new AlluxioURI(path);
    Preconditions.checkArgument(uri.getAuthority() != null, "authority may not be null");
    Preconditions.checkArgument(
        PathUtils.normalizePath(uri.getPath(), AlluxioURI.SEPARATOR).equals(AlluxioURI.SEPARATOR),
        "path should be empty");
    return new ForkUnderFileSystem(ufsConf);
  }

  @Override
  public boolean supportsPath(String path) {
    return (path != null) && path.startsWith("alluxio-fork://");
  }
}
