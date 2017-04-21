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

package alluxio.client;

import alluxio.Constants;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The spec for physical block and chunk layout.
 */
@ThreadSafe
public final class LayoutSpec {
  private int mBlockHeaderSize;
  private int mBlockFooterSize;
  private int mLogicalBlockSize;
  private int mChunkHeaderSize;
  private int mChunkSize;
  private int mChunkFooterSize;

  /**
   * Constructs a new {@link LayoutSpec} with default chunk size.
   *
   * @param blockHeaderSize the block header size
   * @param blockFooterSize the block footer size
   * @param logicalBlockSize the logical block size
   */
  public LayoutSpec(int blockHeaderSize, int blockFooterSize, int logicalBlockSize) {
    this(blockHeaderSize, blockFooterSize, logicalBlockSize, Constants.DEFAULT_CHUNK_HEADER_SIZE,
        Constants.DEFAULT_CHUNK_SIZE, Constants.DEFAULT_CHUNK_FOOTER_SIZE);
  }

  /**
   * Constructs a new {@link LayoutSpec}.
   *
   * @param blockHeaderSize the block header size
   * @param blockFooterSize the block footer size
   * @param logicalBlockSize the logical block size
   * @param chunkHeaderSize the chunk header size
   * @param chunkSize the chunk size
   * @param chunkFooterSize the chunk footer size
   */
  public LayoutSpec(int blockHeaderSize, int blockFooterSize, int logicalBlockSize,
      int chunkHeaderSize, int chunkSize, int chunkFooterSize) {
    Preconditions.checkState(logicalBlockSize % chunkSize == 0);
    mBlockHeaderSize = blockHeaderSize;
    mBlockFooterSize = blockFooterSize;
    mLogicalBlockSize = logicalBlockSize;
    mChunkHeaderSize = chunkHeaderSize;
    mChunkSize = chunkSize;
    mChunkFooterSize = chunkFooterSize;
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
   * @return the logical block size
   */
  public int getLogicalBlockSize() {
    return mLogicalBlockSize;
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
}
