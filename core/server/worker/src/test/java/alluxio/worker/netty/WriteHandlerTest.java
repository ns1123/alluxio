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

package alluxio.worker.netty;

import alluxio.Constants;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status.PStatus;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.proto.ProtoMessage;

import com.google.common.base.Function;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

/**
 * Unit tests for {@link AbstractWriteHandler}.
 */
public abstract class WriteHandlerTest {
  private static final Random RANDOM = new Random();
  protected static final int PACKET_SIZE = 1024;
  protected EmbeddedChannel mChannel;

  /** The file used to hold the data written by the test. */
  protected String mFile;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Test
  public void writeEmptyFile() throws Exception {
    mChannel.writeInbound(newEofRequest(0));

    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.OK, writeResponse);
  }

  @Test
  public void writeNonEmptyFile() throws Exception {
    long len = 0;
    long checksum = 0;
    for (int i = 0; i < 128; i++) {
      DataBuffer dataBuffer = newDataBuffer(PACKET_SIZE);
      checksum += getChecksum(dataBuffer);
      mChannel.writeInbound(newWriteRequest(len, dataBuffer));
      len += PACKET_SIZE;
    }
    // EOF.
    mChannel.writeInbound(newEofRequest(len));

    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.OK, writeResponse);
    checkFileContent(checksum, len);
  }

  @Test
  public void cancel() throws Exception {
    long len = 0;
    long checksum = 0;
    for (int i = 0; i < 1; i++) {
      DataBuffer dataBuffer = newDataBuffer(PACKET_SIZE);
      checksum += getChecksum(dataBuffer);
      mChannel.writeInbound(newWriteRequest(len, dataBuffer));
      len += PACKET_SIZE;
    }
    // Cancel.
    mChannel.writeInbound(newCancelRequest(len));

    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.CANCELED, writeResponse);
    // Our current implementation does not really abort the file when the write is cancelled.
    // The client issues another request to block worker to abort it.
    checkFileContent(checksum, len);
  }

  @Test
  public void writeInvalidOffset() throws Exception {
    mChannel.writeInbound(newWriteRequest(0, newDataBuffer(PACKET_SIZE)));
    // The write request contains an invalid offset
    mChannel.writeInbound(newWriteRequest(PACKET_SIZE + 1, newDataBuffer(PACKET_SIZE)));
    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.INVALID_ARGUMENT, writeResponse);
  }

  @Test
  public void UnregisteredChannelFired() throws Exception {
    ChannelPipeline p = mChannel.pipeline();
    p.fireChannelUnregistered();
  }

  /**
   * Checks the given write response is expected and matches the given error code.
   *
   * @param statusExpected the expected status code
   * @param writeResponse the write response
   */
  protected void checkWriteResponse(PStatus statusExpected, Object writeResponse) {
    Assert.assertTrue(writeResponse instanceof RPCProtoMessage);
    ProtoMessage response = ((RPCProtoMessage) writeResponse).getMessage();
    Assert.assertTrue(response.isResponse());
    Assert.assertEquals(statusExpected, response.asResponse().getStatus());
  }

  /**
   * Checks the file content matches expectation (file length and file checksum).
   *
   * @param expectedChecksum the expected checksum of the file
   * @param size the file size in bytes
   */
  protected void checkFileContent(long expectedChecksum, long size) throws IOException {
    long actualChecksum = 0;
    long actualSize = 0;

    byte[] buffer = new byte[(int) Math.min(Constants.KB, size)];
    int bytesRead;
    try (RandomAccessFile file = new RandomAccessFile(mFile, "r")) {
      do {
        bytesRead = file.read(buffer);
        for (int i = 0; i < bytesRead; i++) {
          actualChecksum += BufferUtils.byteToInt(buffer[i]);
          actualSize++;
        }
      } while (bytesRead >= 0);
    }

    Assert.assertEquals(expectedChecksum, actualChecksum);
    Assert.assertEquals(size, actualSize);
  }

  /**
   * Waits for a response.
   *
   * @return the response
   */
  protected Object waitForResponse(final EmbeddedChannel channel) {
    return CommonUtils.waitForResult("response from the channel.", new Function<Void, Object>() {
      @Override
      public Object apply(Void v) {
        return channel.readOutbound();
      }
    }, WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }

  /**
   * Builds the write request.
   *
   * @param offset the offset
   * @param buffer the data to write
   * @return the write request
   */
  protected RPCProtoMessage newWriteRequest(long offset, DataBuffer buffer) {
    Protocol.WriteRequest writeRequest =
        Protocol.WriteRequest.newBuilder().setId(1L).setOffset(offset)
            .setType(getWriteRequestType()).build();
    return new RPCProtoMessage(new ProtoMessage(writeRequest), buffer);
  }

  /**
   * @param offset the offset
   *
   * @return a new Eof write request
   */
  protected RPCProtoMessage newEofRequest(long offset) {
    return new RPCProtoMessage(new ProtoMessage(
        Protocol.WriteRequest.newBuilder().setOffset(offset).setEof(true)
            .setType(getWriteRequestType()).build()), null);
  }

  /**
   * @param offset the offset
   *
   * @return a new cancel write request
   */
  protected RPCProtoMessage newCancelRequest(long offset) {
    return new RPCProtoMessage(new ProtoMessage(
        Protocol.WriteRequest.newBuilder().setOffset(offset).setCancel(true)
            .setType(getWriteRequestType()).build()), null);
  }

  /**
   * @return the write type of the request
   */
  protected abstract Protocol.RequestType getWriteRequestType();

  /**
   * @param len length of the data buffer
   * @return a newly created data buffer
   */
  protected DataBuffer newDataBuffer(int len) {
    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(len);
    for (int i = 0; i < len; i++) {
      byte value = (byte) (RANDOM.nextInt() % Byte.MAX_VALUE);
      buf.writeByte(value);
    }
    return new DataNettyBufferV2(buf);
  }

  /**
   * @param buffer buffer to get checksum
   * @return the checksum
   */
  public static long getChecksum(DataBuffer buffer) {
    return getChecksum((ByteBuf) buffer.getNettyOutput());
  }

  /**
   * @param buffer buffer to get checksum
   * @return the checksum
   */
  public static long getChecksum(ByteBuf buffer) {
    long ret = 0;
    for (int i = 0; i < buffer.capacity(); i++) {
      ret += BufferUtils.byteToInt(buffer.getByte(i));
    }
    return ret;
  }
}
