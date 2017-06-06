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

package alluxio.client.file;

import alluxio.client.LayoutUtils;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.security.CryptoUtils;
import alluxio.exception.PreconditionMessage;
import alluxio.proto.security.EncryptionProto;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A decorator of {@link FileInStream} with decryption. A crypto buffer is maintained to store
 * the plaintext of the current decryption chunk. Once the buffer is fully consumed, move forward
 * to decrypt and store the next chunk.
 */
@NotThreadSafe
public final class CryptoFileInStream extends FileInStream {
  private final EncryptionProto.Meta mMeta;
  private ByteBuf mCryptoBuf;
  private long mLogicalFileLength;
  private long mLogicalChunkStart;
  private long mLogicalPos;

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   * @param context file system context
   * @return the created {@link CryptoFileInStream} instance
   */
  public static CryptoFileInStream create(
      URIStatus status, InStreamOptions options, FileSystemContext context) {
    return new CryptoFileInStream(status, options, context);
  }

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   * @param context file system context
   */
  protected CryptoFileInStream(
      URIStatus status, InStreamOptions options, FileSystemContext context) {
    super(status, options, context);
    mMeta = options.getEncryptionMeta();
    mLogicalFileLength = LayoutUtils.toLogicalFileLength(mMeta, status.getLength());
  }

  @Override
  public void close() throws IOException {
    if (mCryptoBuf != null) {
      mCryptoBuf.release();
      mCryptoBuf = null;
    }
    // Need an extra step to seek the physical pos to the physical max length, to make sure
    // the cached streams are in expected state.
    super.seek(super.maxSeekPosition());
    super.close();
  }

  @Override
  public int read() throws IOException {
    if (remaining() <= 0) {
      return -1;
    }
    if (mCryptoBuf == null || mCryptoBuf.readableBytes() == 0) {
      getNextCryptoBuf();
      if (mCryptoBuf == null) {
        return -1;
      }
    }
    mLogicalPos++;
    return BufferUtils.byteToInt(mCryptoBuf.readByte());
  }

  @Override
  public int read(byte[] b) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    } else if (remaining() <= 0) {
      return -1;
    }
    int tLen = len;
    int tOff = off;
    int bytesRead = 0;
    while (tLen > 0 && mLogicalPos < mLogicalFileLength) {
      if (mCryptoBuf == null || mCryptoBuf.readableBytes() == 0) {
        getNextCryptoBuf();
        if (mCryptoBuf == null) {
          break;
        }
      }
      long currentBufLeftBytes = mCryptoBuf.readableBytes();
      if (currentBufLeftBytes >= tLen) {
        mCryptoBuf.readBytes(b, tOff, tLen);
        bytesRead += tLen;
        mLogicalPos += tLen;
        tOff += tLen;
        tLen = 0;
      } else {
        mCryptoBuf.readBytes(b, tOff, (int) currentBufLeftBytes);
        bytesRead += currentBufLeftBytes;
        mLogicalPos += currentBufLeftBytes;
        tOff += currentBufLeftBytes;
        tLen -= currentBufLeftBytes;
      }
    }
    return bytesRead;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    if (pos < 0 || pos >= mLogicalFileLength) {
      return -1;
    }
    long physicalChunkStart = LayoutUtils.getPhysicalChunkStart(mMeta, pos);
    // logical read length is either "len" or from pos to the end of the file.
    int logicalToRead = (int) Math.min(len, mLogicalFileLength - pos);

    int tBytesLeft = logicalToRead;
    int tOff = off;
    long tPos = pos;
    while (tBytesLeft > 0) {
      long tLogicalChunkStart = LayoutUtils.getLogicalChunkStart(mMeta, tPos);
      int physicalReadLength = (int) (mMeta.getChunkHeaderSize() + mMeta.getChunkFooterSize()
          + Math.min(mLogicalFileLength - tLogicalChunkStart, mMeta.getChunkSize()));
      byte[] ciphertext = new byte[physicalReadLength];
      super.positionedRead(physicalChunkStart, ciphertext, 0, physicalReadLength);
      physicalChunkStart += physicalReadLength;
      ByteBuf plaintext = CryptoUtils.decryptChunks(mMeta, Unpooled.wrappedBuffer(ciphertext));
      try {
        int offsetFromChunkStart = (int) LayoutUtils.getLogicalOffsetFromChunkStart(mMeta, tPos);
        if (offsetFromChunkStart > 0) {
          plaintext.readerIndex(offsetFromChunkStart);
        }
        int byteRead = Math.min(tBytesLeft, plaintext.readableBytes());
        plaintext.readBytes(b, tOff, byteRead);
        tPos += byteRead;
        tOff += byteRead;
        tBytesLeft -= byteRead;
      } finally {
        plaintext.release();
      }
    }
    return logicalToRead;
  }

  @Override
  public long remaining() {
    return mLogicalFileLength - mLogicalPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mLogicalPos == pos) {
      return;
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLogicalFileLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);
    seekLogicalInternal(pos);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, remaining());
    seek(mLogicalPos + toSkip);
    return toSkip;
  }

  private void seekLogicalInternal(long pos) throws IOException {
    long seekChunkStart = LayoutUtils.getLogicalChunkStart(mMeta, pos);
    // No need to get a new CryptoBuf if the seek pos is already in the current buf
    if (mCryptoBuf == null || seekChunkStart != mLogicalChunkStart - mCryptoBuf.capacity()) {
      mLogicalChunkStart = seekChunkStart;
      super.seek(LayoutUtils.getPhysicalChunkStart(mMeta, pos));
      getNextCryptoBuf();
      if (mCryptoBuf == null) {
        Preconditions.checkState(mLogicalChunkStart == mLogicalFileLength);
        mLogicalPos = pos;
        super.seek(super.maxSeekPosition());
        return;
      }
    }
    if (mCryptoBuf != null) {
      int offsetFromChunkStart = (int) LayoutUtils.getLogicalOffsetFromChunkStart(mMeta, pos);
      mCryptoBuf.readerIndex(offsetFromChunkStart);
    }
    mLogicalPos = pos;
  }

  private void getNextCryptoBuf() throws IOException {
    mCryptoBuf = readBufferAndDecrypt();
  }

  private ByteBuf readBufferAndDecrypt() throws IOException {
    if (mLogicalFileLength - mLogicalChunkStart < 0) {
      throw new IOException(String.format(
          "Failed to create a new crypto buffer: current logical chunk start %d reaches the"
          + "logical EOF %d.", mLogicalChunkStart, mLogicalFileLength));
    }
    if (mLogicalFileLength == mLogicalChunkStart) {
      // reaches the logical file EOF. This is legal when seek to logical EOF.
      return null;
    }
    // TODO(chaomin): tune the crypto buffer size to batch multiple chunks decryption
    int physicalChunkSize = (int) (mMeta.getChunkHeaderSize() + mMeta.getChunkFooterSize()
        + Math.min(mLogicalFileLength - mLogicalChunkStart, mMeta.getChunkSize()));
    byte[] cipherChunk = new byte[physicalChunkSize];
    int ciphertextSize = super.readInternal(cipherChunk, 0, physicalChunkSize);
    if (ciphertextSize <= 0) {
      throw new IOException("Failed to create a new crypto buffer: no ciphertext was read.");
    }
    ByteBuf cipherBuf = Unpooled.wrappedBuffer(cipherChunk);
    try {
      ByteBuf plaintext = CryptoUtils.decryptChunks(mMeta, cipherBuf);
      mLogicalChunkStart += plaintext.readableBytes();
      return plaintext;
    } finally {
      cipherBuf.release();
    }
  }
}
