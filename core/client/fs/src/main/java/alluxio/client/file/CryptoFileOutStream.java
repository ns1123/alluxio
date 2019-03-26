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

import alluxio.AlluxioURI;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.security.CryptoUtils;
import alluxio.exception.PreconditionMessage;
import alluxio.proto.security.EncryptionProto;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A decorator of {@link FileOutStream} with encryption. A crypto buffer is maintained to store the
 * plaintext for an encryption chunk. Once the buffer is full, encrypt and flush to the underlying
 * out streams.
 */
@NotThreadSafe
public final class CryptoFileOutStream extends FileOutStream {
  private final EncryptionProto.Meta mMeta;
  private ByteBuf mCryptoBuf;
  private boolean mClosed;

  /**
   * Creates a new {@link CryptoFileOutStream}.
   *
   * @param path the file path
   * @param options the client options
   * @param context the file system context
   */
  public CryptoFileOutStream(AlluxioURI path, OutStreamOptions options, FileSystemContext context)
      throws IOException {
    super(path, options, context);
    Preconditions.checkState(options.isEncrypted());
    mMeta = options.getEncryptionMeta();
    // TODO(chaomin): tune the crypto buffer size to batch multiple chunks encryption
    mCryptoBuf = PooledByteBufAllocator.DEFAULT.buffer((int) mMeta.getChunkSize());
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    try {
      flushCryptoBuf();
      // Write the file footer in plaintext.
      writeFileFooter();
      mClosed = true;
      super.close();
    } finally {
      mCryptoBuf.release();
    }
  }

  @Override
  public void flush() throws IOException {
    // Note: Flush at non-chunk-boundary is a no-op with GCM encryption mode. Alluxio will
    // never flush a partial logical chunk.
    if (mCryptoBuf != null && mCryptoBuf.writableBytes() == 0) {
      flushCryptoBuf();
    }
    super.flush();
  }

  @Override
  public void write(int b) throws IOException {
    if (mCryptoBuf.writableBytes() == 0) {
      getNextCryptoBuf();
    }
    mCryptoBuf.writeByte(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    int tLen = len;
    int tOff = off;
    while (tLen > 0) {
      if (mCryptoBuf.writableBytes() == 0) {
        getNextCryptoBuf();
      }
      long currentBufLeftBytes = mCryptoBuf.writableBytes();
      if (currentBufLeftBytes >= tLen) {
        mCryptoBuf.writeBytes(b, tOff, tLen);
        tLen = 0;
      } else {
        mCryptoBuf.writeBytes(b, tOff, (int) currentBufLeftBytes);
        tOff += currentBufLeftBytes;
        tLen -= currentBufLeftBytes;
      }
    }
  }

  private void flushCryptoBuf() throws IOException {
    encryptBufferAndWrite();
    updateCryptoBuf();
  }

  private void getNextCryptoBuf() throws IOException {
    flushCryptoBuf();
  }

  private void encryptBufferAndWrite() throws IOException {
    if (mCryptoBuf.readableBytes() == 0) {
      return;
    }
    ByteBuf encryptedBuf = CryptoUtils.encryptChunks(mMeta, mCryptoBuf);
    try {
      writeInternal(encryptedBuf, 0, encryptedBuf.readableBytes());
    } finally {
      encryptedBuf.release();
    }
  }

  private void updateCryptoBuf() {
    mCryptoBuf.discardReadBytes();
    mCryptoBuf.resetWriterIndex();
    mCryptoBuf.resetReaderIndex();
  }

  private void writeFileFooter() throws IOException {
    byte[] footer = alluxio.client.LayoutUtils.encodeFooter(mMeta);
    super.write(footer);
  }
}