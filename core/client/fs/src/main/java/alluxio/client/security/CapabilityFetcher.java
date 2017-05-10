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
public class CapabilityFetcher {
  private final FileSystemContext mFileSystemContext;
  private final AlluxioURI mPath;

  private Capability mCapability;

  /**
   * Creates an instance of {@link CapabilityFetcher}.
   *
   * @param context the file system context
   * @param path the file path
   * @param capability the capability
   */
  public CapabilityFetcher(FileSystemContext context, String path, Capability capability) {
    Preconditions.checkNotNull(context);
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(capability);
    mFileSystemContext = context;
    mPath = new AlluxioURI(path);
    mCapability = capability;
  }

  /**
   * Updates the capability.
   *
   * @return the updated capability
   * @throws IOException if there is any IO related failure
   * @throws AlluxioException if there is any Alluxio related failure
   */
  public Capability update() throws IOException, AlluxioException {
    FileSystemMasterClient client = mFileSystemContext.acquireMasterClient();
    try {
      URIStatus status = client.getStatus(mPath);
      Capability capability = status.getCapability();
      synchronized (this) {
        mCapability = Preconditions.checkNotNull(capability);
      }
      return capability;
    } finally {
      mFileSystemContext.releaseMasterClient(client);
    }
  }

  /**
   * @return the current capability
   */
  public Capability getCapability() {
    synchronized (this) {
      return mCapability;
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
    return Objects.equal(mPath, ((CapabilityFetcher) o).mPath) && Objects
        .equal(getCapability(), ((CapabilityFetcher) o).getCapability());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("path", mPath).add("capability", getCapability())
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPath);
  }
}
