/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.UnderFileSystem;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class provides a streaming API to read a fixed chunk from a file in the under storage
 * system. The user may freely seek and skip within the fixed chunk of data. The under storage
 * system read does not guarantee any locality and is dependent on the implementation of the under
 * storage client.
 */
@NotThreadSafe
public final class UnderStoreBlockInStream extends BlockInStream {
  private final long mInitPos;
  /**
   * The length of this current block. This may be
   * {@link Constants#UNKNOWN_SIZE}. See {@link #getLength()} for more length information.
   */
  private final long mLength;
  private final String mUfsPath;
  /**
   * The maximum possible length for this block. This is usually equal to {@link #mLength}, but when
   * {@link #mLength} is {@link Constants#UNKNOWN_SIZE}, this {@link #mMaxLength} will be the
   * block size of the file. See {@link #getLength()} for more length information.
   */
  private final long mMaxLength;

  private long mPos;
  private InputStream mUnderStoreStream;
  /**
   * The computed length of the block. This is only computed when the UFS stream is done. See
   * {@link #getLength()} for more length information.
   */
  private long mComputedLength;

  /**
   * Creates a new under storage file input stream.
   *
   * @param initPos the initial position
   * @param length the length of this current block (allowed to be {@link Constants#UNKNOWN_SIZE})
   * @param maxLength the max possible length of this block, which is the block size for the file
   * @param ufsPath the under file system path
   * @throws IOException if an I/O error occurs
   */
  public UnderStoreBlockInStream(long initPos, long length, long maxLength, String ufsPath)
      throws IOException {
    mInitPos = initPos;
    mLength = length;
    mMaxLength = maxLength;
    mUfsPath = ufsPath;
    mComputedLength = Constants.UNKNOWN_SIZE;
    setUnderStoreStream(initPos);
  }

  @Override
  public void close() throws IOException {
    mUnderStoreStream.close();
  }

  @Override
  public int read() throws IOException {
    int data = mUnderStoreStream.read();
    if (data == -1) {
      // End of stream. Compute the length.
      mComputedLength = mPos - mInitPos;
      return data;
    }
    mPos++;
    return data;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = mUnderStoreStream.read(b, off, len);
    if (bytesRead == -1) {
      // End of stream. Compute the length.
      mComputedLength = mPos - mInitPos;
      return bytesRead;
    }
    mPos += bytesRead;
    return bytesRead;
  }

  @Override
  public long remaining() {
    return mInitPos + getLength() - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    long offset = mPos - mInitPos;
    if (pos < offset) {
      setUnderStoreStream(pos);
    } else {
      long toSkip = pos - offset;
      if (skip(toSkip) != toSkip) {
        throw new IOException(ExceptionMessage.FAILED_SEEK_FORWARD.getMessage(pos));
      }
    }
  }

  @Override
  public long skip(long n) throws IOException {
    // Negative skip returns 0
    if (n <= 0) {
      return 0;
    }
    // Cannot skip beyond boundary
    long toSkip = Math.min(mInitPos + getLength() - mPos, n);
    long skipped = mUnderStoreStream.skip(toSkip);
    if (toSkip != skipped) {
      throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(toSkip));
    }
    mPos += skipped;
    return skipped;
  }

  private void setUnderStoreStream(long pos) throws IOException {
    if (mUnderStoreStream != null) {
      mUnderStoreStream.close();
    }
    UnderFileSystem ufs = UnderFileSystem.get(mUfsPath, ClientContext.getConf());
    mUnderStoreStream = ufs.open(mUfsPath);
    mPos = 0;
    if (mPos != pos && pos != skip(pos)) {
      throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(pos));
    }
  }

  /**
   * Returns the length of the current UFS block. This method handles the situation when the UFS
   * file has an unknown length. If the UFS file has an unknown length, the length returned will
   * be the block size, or the computed size (if it is valid). The computed size is valid only
   * once the UFS stream is read completely.
   *
   * @return the length of this current block
   */
  private long getLength() {
    if (mLength != Constants.UNKNOWN_SIZE) {
      return mLength;
    }
    // The length of the stream was initially unknown.
    if (mComputedLength != Constants.UNKNOWN_SIZE) {
      // If the length was unknown, but the computed length is known (UFS stream completed), use
      // the computed length.
      return mComputedLength;
    }
    // The length is unknown. Use the max block size until the computed length is known.
    return mMaxLength;
  }
}
