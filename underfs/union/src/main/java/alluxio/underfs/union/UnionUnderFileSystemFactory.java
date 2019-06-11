/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link UnionUnderFileSystem}.
 *
 * The URI used for creating a new instance of {@link UnionUnderFileSystem} is expected to use
 * the authority to specify an "alias" for the mount point. This makes it possible for Alluxio
 * to distinguish between different instances of {@link UnionUnderFileSystem} mounted to Alluxio.
 */
@ThreadSafe
public class UnionUnderFileSystemFactory implements UnderFileSystemFactory {
  /**
   * Constructs a new {@link UnionUnderFileSystemFactory}.
   */
  public UnionUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration ufsConf) {
    Preconditions.checkArgument(path != null, "path may not be null");
    AlluxioURI uri = new AlluxioURI(path);
    Preconditions.checkArgument(uri.getAuthority() != null, "authority may not be null");
    return new UnionUnderFileSystem(ufsConf);
  }

  @Override
  public boolean supportsPath(String path) {
    return (path != null) && path.startsWith(String.format("%s://", UnionUnderFileSystem.SCHEME));
  }
}
