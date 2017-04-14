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

  private int mBlockHeaderSize;
  private int mBlockFooterSize;
  private int mChunkHeaderSize = Constants.DEFAULT_CHUNK_HEADER_SIZE;
  private int mChunkSize = Constants.DEFAULT_CHUNK_SIZE;
  private int mChunkFooterSize = Constants.DEFAULT_CHUNK_FOOTER_SIZE;
  private long mEncryptionId = Constants.INVALID_ENCRYPTION_ID;

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
    mBlockHeaderSize = blockHeader.getBlockHeaderSize();
    mBlockFooterSize = blockHeader.getBlockFooterSize();
    mChunkHeaderSize = blockHeader.getChunkHeaderSize();
    mChunkSize = blockHeader.getChunkSize();
    mChunkFooterSize = blockHeader.getChunkFooterSize();
    mEncryptionId = blockHeader.getEncryptionId();
  }

  /**
   * @return the block header size
   */
  public int getBlockHeaderSize() {
    return mBlockHeaderSize;
  }

  /**
   * @return the block footer size
   */
  public int getBlockFooterSize() {
    return mBlockFooterSize;
  }

  /**
   * @return the chunk header size
   */
  public int getChunkHeaderSize() {
    return mChunkHeaderSize;
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
   * @return the encryption id
   */
  public long getEncryptionId() {
    return mEncryptionId;
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
   * @param blockFooterSize the block footer size to set
   * @return the updated object
   */
  public BlockHeader setBlockFooterSize(int blockFooterSize) {
    mBlockFooterSize = blockFooterSize;
    return this;
  }

  /**
   * @param chunkHeaderSize the chunk header size to set
   * @return the updated object
   */
  public BlockHeader setChunkHeaderSize(int chunkHeaderSize) {
    mChunkHeaderSize = chunkHeaderSize;
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
   * @param encryptionId the encryption id to set
   * @return the updated object
   */
  public BlockHeader setEncryptionId(long encryptionId) {
    mEncryptionId = encryptionId;
    return this;
  }

  /**
   * @return thrift representation of the block header
   */
  protected alluxio.thrift.BlockHeader toThrift() {
    alluxio.thrift.BlockHeader header =
        new alluxio.thrift.BlockHeader(
            mBlockHeaderSize, mBlockFooterSize, mChunkHeaderSize, mChunkSize, mChunkFooterSize,
            mEncryptionId);
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
    return mBlockHeaderSize == that.mBlockHeaderSize
        && mBlockFooterSize == that.mBlockFooterSize
        && mChunkHeaderSize == that.mChunkHeaderSize
        && mChunkSize == that.mChunkSize
        && mChunkFooterSize == that.mChunkFooterSize
        && mEncryptionId == that.mEncryptionId;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        mBlockHeaderSize,
        mBlockFooterSize,
        mChunkHeaderSize,
        mChunkSize,
        mChunkFooterSize,
        mEncryptionId
    );
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("encryptionId", mEncryptionId)
        .add("blockHeaderSize", mBlockHeaderSize)
        .add("blockFooterSize", mBlockFooterSize)
        .add("chunkHeaderSize", mChunkHeaderSize)
        .add("chunkSize", mChunkSize)
        .add("chunkFooterSize", mChunkFooterSize).toString();
  }
}
