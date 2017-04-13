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

package alluxio.wire;

import alluxio.Constants;

import com.google.common.base.Objects;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The block header.
 */
@NotThreadSafe
public class BlockHeader implements Serializable {
  private static final long serialVersionUID = 6006843856339963833L;

  private long mEncryptionId = Constants.INVALID_ENCRYPTION_ID;
  private int mBlockHeaderSize;
  private int mChunkSize = Constants.DEFAULT_CHUNK_SIZE;
  private int mChunkFooterSize = Constants.DEFAULT_CHUNK_FOOTER_SIZE;

  /**
   * Creates a new instance of {@link BlockHeader}.
   */
  public BlockHeader() {}

  /**
   * Creates a new instance of {@link BlockHeader} from thrift representation.
   *
   * @param blockHeader the thrift representation of a block header
   */
  protected BlockHeader(alluxio.thrift.BlockHeader blockHeader) {
    mEncryptionId = blockHeader.getEncryptionId();
    mBlockHeaderSize = blockHeader.getBlockHeaderSize();
    mChunkSize = blockHeader.getChunkSize();
    mChunkFooterSize = blockHeader.getChunkFooterSize();
  }

  /**
   * @return the encryption id
   */
  public long getEncryptionId() {
    return mEncryptionId;
  }

  /**
   * @return the block header size
   */
  public int getBlockHeaderSize() {
    return mBlockHeaderSize;
  }

  /**
   * @return the chunk size
   */
  public int getChunkSize() {
    return mChunkSize;
  }

  /**
   * @return the chunk footer size
   */
  public int getChunkFooterSize() {
    return mChunkFooterSize;
  }

  /**
   * @param encryptionId the encryption id to set
   * @return the updated object
   */
  public BlockHeader setEncryptionId(long encryptionId) {
    mEncryptionId = encryptionId;
    return this;
  }

  /**
   * @param blockHeaderSize the block header size to set
   * @return the updated object
   */
  public BlockHeader setBlockHeaderSize(int blockHeaderSize) {
    mBlockHeaderSize = blockHeaderSize;
    return this;
  }

  /**
   * @param chunkSize the chunk size to set
   * @return the updated object
   */
  public BlockHeader setChunkSize(int chunkSize) {
    mChunkSize = chunkSize;
    return this;
  }

  /**
   * @param chunkFooterSize the chunk footer to set
   * @return the updated object
   */
  public BlockHeader setChunkFooterSize(int chunkFooterSize) {
    mChunkFooterSize = chunkFooterSize;
    return this;
  }

  /**
   * @return thrift representation of the block header
   */
  protected alluxio.thrift.BlockHeader toThrift() {
    alluxio.thrift.BlockHeader header =
        new alluxio.thrift.BlockHeader(
            mEncryptionId, mBlockHeaderSize, mChunkSize, mChunkFooterSize);
    return header;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockHeader)) {
      return false;
    }
    BlockHeader that = (BlockHeader) o;
    return mEncryptionId == that.mEncryptionId && mBlockHeaderSize == that.mBlockHeaderSize
        && mChunkSize == that.mChunkSize && mChunkFooterSize == that.mChunkFooterSize;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mEncryptionId, mBlockHeaderSize, mChunkSize, mChunkFooterSize);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("encryptionId", mEncryptionId)
        .add("blockHeaderSize", mBlockHeaderSize).add("chunkSize", mChunkSize)
        .add("chunkFooterSize", mChunkFooterSize).toString();
  }
}
