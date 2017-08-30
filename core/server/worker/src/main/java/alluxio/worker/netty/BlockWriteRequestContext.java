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
import alluxio.worker.block.io.BlockWriter;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The block write request internal representation.
 */
@NotThreadSafe
public final class BlockWriteRequestContext extends WriteRequestContext<BlockWriteRequest> {
  private BlockWriter mBlockWriter;
  private long mBytesReserved;
  // ALLUXIO CS ADD
  private boolean mIsWritingToLocal = true;
  private alluxio.underfs.UnderFileSystem mUnderFileSystem;
  private java.io.OutputStream mOutputStream;
  private String mUfsPath;
  // ALLUXIO CS END

  BlockWriteRequestContext(Protocol.WriteRequest request, long bytesReserved) {
    super(new BlockWriteRequest(request));
    mBytesReserved = bytesReserved;
  }

  /**
   * @return the block writer
   */
  @Nullable
  public BlockWriter getBlockWriter() {
    return mBlockWriter;
  }

  /**
   * @return the bytes reserved
   */
  public long getBytesReserved() {
    return mBytesReserved;
  }

  /**
   * @param blockWriter block writer to set
   */
  public void setBlockWriter(BlockWriter blockWriter) {
    mBlockWriter = blockWriter;
  }

  /**
   * @param bytesReserved the bytes reserved to set
   */
  public void setBytesReserved(long bytesReserved) {
    mBytesReserved = bytesReserved;
  }
  // ALLUXIO CS ADD

  /**
   * @return is the current request writing to UFS
   */
  public boolean isWritingToLocal() {
    return mIsWritingToLocal;
  }

  /**
   * @return the UFS path of the block
   */
  @Nullable
  public String getUfsPath() {
    return mUfsPath;
  }

  /**
   * @return the output stream
   */
  @Nullable
  public java.io.OutputStream getOutputStream() {
    return mOutputStream;
  }
  /**
   * @return the handler of the UFS
   */
  @Nullable
  public alluxio.underfs.UnderFileSystem getUnderFileSystem() {
    return mUnderFileSystem;
  }


  /**
   * @param writingToLocal whether the current request is writing to UFS
   */
  public void setWritingToLocal(boolean writingToLocal) {
    mIsWritingToLocal = writingToLocal;
  }

  /**
   * @param outputStream output stream to set
   */
  public void setOutputStream(java.io.OutputStream outputStream) {
    mOutputStream = outputStream;
  }

  /**
   * @param underFileSystem UFS to set
   */
  public void setUnderFileSystem(alluxio.underfs.UnderFileSystem underFileSystem) {
    mUnderFileSystem = underFileSystem;
  }

  /**
   * @param ufsPath UFS path to set
   */
  public void setUfsPath(String ufsPath) {
    mUfsPath = ufsPath;
  }
  // ALLUXIO CS END
}
