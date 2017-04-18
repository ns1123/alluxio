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
 * The file metadata within file footer.
 */
@NotThreadSafe
public class FileMetadata implements Serializable {
  private static final long serialVersionUID = 6006843856339963833L;

  private int mBlockHeaderSize = Constants.DEFAULT_BLOCK_HEADER_SIZE;
  private int mBlockFooterSize = Constants.DEFAULT_BLOCK_FOOTER_SIZE;
  private int mChunkHeaderSize = Constants.DEFAULT_CHUNK_HEADER_SIZE;
  private int mChunkSize = Constants.DEFAULT_CHUNK_SIZE;
  private int mChunkFooterSize = Constants.DEFAULT_CHUNK_FOOTER_SIZE;
  private int mPhysicalBlockSize;
  private long mEncryptionId = Constants.INVALID_ENCRYPTION_ID;

  /**
   * Creates a new instance of {@link FileMetadata}.
   */
  public FileMetadata() {}

  /**
   * Creates a new instance of {@link FileMetadata} from thrift representation.
   *
   * @param fileMetadata the thrift representation of a file metadata
   */
  protected FileMetadata(alluxio.thrift.FileMetadata fileMetadata) {
    mBlockHeaderSize = fileMetadata.getBlockHeaderSize();
    mBlockFooterSize = fileMetadata.getBlockFooterSize();
    mChunkHeaderSize = fileMetadata.getChunkHeaderSize();
    mChunkSize = fileMetadata.getChunkSize();
    mChunkFooterSize = fileMetadata.getChunkFooterSize();
    mPhysicalBlockSize = fileMetadata.getPhysicalBlockSize();
    mEncryptionId = fileMetadata.getEncryptionId();
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
   * @return the physical block size
   */
  public int getPhysicalBlockSize() {
    return mPhysicalBlockSize;
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
  public FileMetadata setBlockHeaderSize(int blockHeaderSize) {
    mBlockHeaderSize = blockHeaderSize;
    return this;
  }

  /**
   * @param blockFooterSize the block footer size to set
   * @return the updated object
   */
  public FileMetadata setBlockFooterSize(int blockFooterSize) {
    mBlockFooterSize = blockFooterSize;
    return this;
  }

  /**
   * @param chunkHeaderSize the chunk header size to set
   * @return the updated object
   */
  public FileMetadata setChunkHeaderSize(int chunkHeaderSize) {
    mChunkHeaderSize = chunkHeaderSize;
    return this;
  }

  /**
   * @param chunkSize the chunk size to set
   * @return the updated object
   */
  public FileMetadata setChunkSize(int chunkSize) {
    mChunkSize = chunkSize;
    return this;
  }

  /**
   * @param chunkFooterSize the chunk footer to set
   * @return the updated object
   */
  public FileMetadata setChunkFooterSize(int chunkFooterSize) {
    mChunkFooterSize = chunkFooterSize;
    return this;
  }

  /**
   * @param physicalBlockSize the physical block size to set
   * @return the updated object
   */
  public FileMetadata setPhysicalBlockSize(int physicalBlockSize) {
    mPhysicalBlockSize = physicalBlockSize;
    return this;
  }

  /**
   * @param encryptionId the encryption id to set
   * @return the updated object
   */
  public FileMetadata setEncryptionId(long encryptionId) {
    mEncryptionId = encryptionId;
    return this;
  }

  /**
   * @return thrift representation of the block header
   */
  protected alluxio.thrift.FileMetadata toThrift() {
    alluxio.thrift.FileMetadata header =
        new alluxio.thrift.FileMetadata(
            mBlockHeaderSize, mBlockFooterSize, mChunkHeaderSize, mChunkSize, mChunkFooterSize,
            mPhysicalBlockSize, mEncryptionId);
    return header;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileMetadata)) {
      return false;
    }
    FileMetadata that = (FileMetadata) o;
    return mBlockHeaderSize == that.mBlockHeaderSize
        && mBlockFooterSize == that.mBlockFooterSize
        && mChunkHeaderSize == that.mChunkHeaderSize
        && mChunkSize == that.mChunkSize
        && mChunkFooterSize == that.mChunkFooterSize
        && mPhysicalBlockSize == that.mPhysicalBlockSize
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
        mPhysicalBlockSize,
        mEncryptionId
    );
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("blockHeaderSize", mBlockHeaderSize)
        .add("blockFooterSize", mBlockFooterSize)
        .add("chunkHeaderSize", mChunkHeaderSize)
        .add("chunkSize", mChunkSize)
        .add("chunkFooterSize", mChunkFooterSize)
        .add("physicalBlockSize", mPhysicalBlockSize)
        .add("encryptionId", mEncryptionId).toString();
  }
}
