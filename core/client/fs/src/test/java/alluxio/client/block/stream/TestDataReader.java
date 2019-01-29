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

package alluxio.client.block.stream;

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

/**
 * A {@link DataReader} which serves data from a given byte array.
 */
public class TestDataReader implements DataReader {
  private final byte[] mData;
  private long mPos;
  private long mEnd;
<<<<<<< HEAD:core/client/fs/src/test/java/alluxio/client/block/stream/TestPacketReader.java
  // ALLUXIO CS REPLACE
  // private long mPacketSize = 128;
  //
  // public TestPacketReader(byte[] data, long offset, long length) {
  //   mData = data;
  //   mPos = offset;
  //   mEnd = offset + length;
  // }
  // ALLUXIO CS WITH
  private long mPacketSize;

  public TestPacketReader(byte[] data, long offset, long length) {
    this(data, offset, length, 128);
  }

  public TestPacketReader(byte[] data, long offset, long length, long packetSize) {
=======
  private long mChunkSize = 128;

  public TestDataReader(byte[] data, long offset, long length) {
>>>>>>> 8cc5a292f4c6e38ed0066ce5bd700cc946dc3803:core/client/fs/src/test/java/alluxio/client/block/stream/TestDataReader.java
    mData = data;
    mPos = offset;
    mEnd = offset + length;
    mPacketSize = packetSize;
  }
  // ALLUXIO CS END

  @Override
  @Nullable
  public DataBuffer readChunk() {
    if (mPos >= mEnd || mPos >= mData.length) {
      return null;
    }
    int bytesToRead = (int) (Math.min(Math.min(mChunkSize, mEnd - mPos), mData.length - mPos));
    ByteBuffer buffer = ByteBuffer.wrap(mData, (int) mPos, bytesToRead);
    DataBuffer dataBuffer = new DataByteBuffer(buffer, buffer.remaining());
    mPos += dataBuffer.getLength();
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() { }
}
