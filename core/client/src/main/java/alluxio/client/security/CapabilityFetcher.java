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

package alluxio.client.security;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.security.capability.Capability;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * This implements a {@link Callable} that returns a {@link Capability} for a given file once
 * called.
 */
public class CapabilityFetcher implements Callable<Capability> {
  private final FileSystemContext mFileSystemContext;
  private final AlluxioURI mPath;

  /**
   * Creates an instance of {@link CapabilityFetcher}.
   *
   * @param context the file system context
   * @param path the file path
   */
  public CapabilityFetcher(FileSystemContext context, String path) {
    Preconditions.checkNotNull(context);
    Preconditions.checkNotNull(path);
    mFileSystemContext = context;
    mPath = new AlluxioURI(path);
  }

  @Override
  public Capability call() throws IOException, AlluxioException {
    FileSystemMasterClient client = mFileSystemContext.acquireMasterClient();
    try {
      URIStatus status = client.getStatus(mPath);
      return status.getCapability();
    } finally {
      mFileSystemContext.releaseMasterClient(client);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CapabilityFetcher)) {
      return false;
    }
    return Objects.equal(mPath, ((CapabilityFetcher) o).mPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPath);
  }
}
