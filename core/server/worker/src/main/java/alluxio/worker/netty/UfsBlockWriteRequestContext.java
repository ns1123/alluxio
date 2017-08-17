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

package alluxio.worker.netty;

import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UnderFileSystem;

import com.google.common.base.Preconditions;

import java.io.OutputStream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The block write request internal representation.
 */
@NotThreadSafe
public final class UfsBlockWriteRequestContext extends WriteRequestContext<UfsBlockWriteRequest> {
  private UnderFileSystem mUnderFileSystem;
  private OutputStream mOutputStream;
  private String mUfsPath;

  UfsBlockWriteRequestContext(Protocol.WriteRequest request) {
    super(new UfsBlockWriteRequest(request));
    Preconditions.checkState(request.getOffset() == 0);
  }

  /**
   * @return the output stream
   */
  @Nullable
  public OutputStream getOutputStream() {
    return mOutputStream;
  }

  /**
   * @return the handler of the UFS
   */
  @Nullable
  public UnderFileSystem getUnderFileSystem() {
    return mUnderFileSystem;
  }

  /**
   * @return the UFS path of the block
   */
  @Nullable
  public String getUfsPath() {
    return mUfsPath;
  }

  /**
   * @param outputStream output stream to set
   */
  public void setOutputStream(OutputStream outputStream) {
    mOutputStream = outputStream;
  }

  /**
   * @param underFileSystem UFS to set
   */
  public void setUnderFileSystem(UnderFileSystem underFileSystem) {
    mUnderFileSystem = underFileSystem;
  }

  /**
   * @param ufsPath UFS path to set
   */
  public void setUfsPath(String ufsPath) {
    mUfsPath = ufsPath;
  }
}
