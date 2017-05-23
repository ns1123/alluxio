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
public class CryptoFileInStream extends FileInStream {
  private EncryptionProto.Meta mMeta;
  private ByteBuf mCryptoBuf;
  private long mLogicalFileLength;
  private long mLogicalChunkOff;
  private long mLogicalPos;

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   * @param context file system context
   * @return the created {@link CryptoFileInStream} instance
   */
  public static CryptoFileInStream create(URIStatus status, InStreamOptions options,
                                          FileSystemContext context) {
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
    mCryptoBuf = null;
    final int footerSize = (int) mMeta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
    mLogicalFileLength = LayoutUtils.toLogicalLength(mMeta, 0L, status.getLength() - footerSize);
    mLogicalPos = 0;
  }

  @Override
  public void close() throws IOException {
    if (mCryptoBuf != null) {
      mCryptoBuf.release();
      mCryptoBuf = null;
    }
    super.close();
  }

  @Override
  public int read() throws IOException {
    if (mCryptoBuf == null || mCryptoBuf.readableBytes() == 0) {
      getNextCryptoBuf();
    }
    mLogicalPos++;
    return mCryptoBuf.readByte();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (remaining() <= 0) {
      return -1;
    }
    int tLen = len;
    int tOff = off;
    int bytesRead = 0;
    while (tLen > 0 && mLogicalPos < mLogicalFileLength) {
      if (mCryptoBuf == null || mCryptoBuf.readableBytes() == 0) {
        getNextCryptoBuf();
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
    // TODO(chaomin): is it different when cache partial block is on?
    seek(pos);
    return read(b, off, len);
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
    mLogicalChunkOff = LayoutUtils.getPhysicalChunkStart(mMeta, pos);
    int offsetFromChunkStart = (int) LayoutUtils.getPhysicalOffsetFromChunkStart(mMeta, pos);
    getNextCryptoBuf();
    mCryptoBuf.readerIndex(offsetFromChunkStart);
    mLogicalPos = pos;
  }

  private void getNextCryptoBuf() throws IOException {
    mCryptoBuf = readBufferAndDecrypt();
  }

  private ByteBuf readBufferAndDecrypt() throws IOException {
    if (mLogicalFileLength - mLogicalChunkOff <= 0) {
      close();
      return null;
    }
    int physicalChunkSize =
        (int) (mMeta.getChunkHeaderSize() + mMeta.getChunkFooterSize()
        + Math.min(mLogicalFileLength - mLogicalChunkOff, mMeta.getChunkSize()));
    byte[] cipherChunk = new byte[physicalChunkSize];
    int ciphertextSize = super.readInternal(cipherChunk, 0, physicalChunkSize);
    if (ciphertextSize <= 0) {
      return null;
    }
    ByteBuf cipherBuf = Unpooled.wrappedBuffer(cipherChunk);
    ByteBuf plaintext = CryptoUtils.decryptChunks(mMeta, cipherBuf);
    cipherBuf.release();
    mLogicalChunkOff += plaintext.readableBytes();
    return plaintext;
  }
}
