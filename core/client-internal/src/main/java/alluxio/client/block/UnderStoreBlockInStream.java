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
  private final long mLength;
  private final String mUfsPath;
  /** If true, the block size is not known beforehand. */
  private final boolean mUnknownLength;

  private long mPos;
  private InputStream mUnderStoreStream;
  /** The computed length of the block. This is only computed when the UFS stream is done. */
  private long mComputedLength;

  /**
   * Creates a new under storage file input stream.
   *
   * @param initPos the initial position
   * @param length the length
   * @param ufsPath the under file system path
   * @return the created {@link UnderStoreBlockInStream} instance
   * @throws IOException if an I/O error occurs
   */
  public static UnderStoreBlockInStream create(long initPos, long length, String ufsPath)
      throws IOException {
    return new UnderStoreBlockInStream(initPos, length, false, ufsPath);
  }

  /**
   * Creates a new under storage file input stream, with an unknown length.
   *
   * @param initPos the initial position
   * @param maximumLength the maximum length
   * @param ufsPath the under file system path
   * @return the created {@link UnderStoreBlockInStream} instance
   * @throws IOException if an I/O error occurs
   */
  public static UnderStoreBlockInStream createWithUnknownLength(long initPos, long maximumLength,
      String ufsPath) throws IOException {
    return new UnderStoreBlockInStream(initPos, maximumLength, true, ufsPath);
  }

  /**
   * Creates a new under storage file input stream.
   *
   * @param initPos the initial position
   * @param length the length
   * @param unknownLength if true, the length is not known, so the specified length is the maximum
   * @param ufsPath the under file system path
   * @throws IOException if an I/O error occurs
   */
  private UnderStoreBlockInStream(long initPos, long length, boolean unknownLength, String ufsPath)
      throws IOException {
    mInitPos = initPos;
    mLength = length;
    mUfsPath = ufsPath;
    mUnknownLength = unknownLength;
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
      // End of stream.
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
      // End of stream.
      mComputedLength = mPos - mInitPos;
      return bytesRead;
    }
    mPos += bytesRead;
    return bytesRead;
  }

  @Override
  public long remaining() {
    if (mUnknownLength && mComputedLength != Constants.UNKNOWN_SIZE) {
      // If the length was unknown, but the computed length is known (UFS stream completed), use
      // the computed length.
      return mInitPos + mComputedLength - mPos;
    }
    return mInitPos + mLength - mPos;
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
    long toSkip = Math.min(mInitPos + mLength - mPos, n);
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
}
