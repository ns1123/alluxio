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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.BoundedStream;
import alluxio.client.Cancelable;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides an {@link OutputStream} implementation that is based on {@link PacketWriter} which
 * streams data packet by packet.
 */
@NotThreadSafe
public class PacketOutStream extends OutputStream implements BoundedStream, Cancelable {
  private final Closer mCloser;
  /** Length of the stream. If unknown, set to Long.MAX_VALUE. */
  private final long mLength;
  private ByteBuf mCurrentPacket = null;

  private final List<PacketWriter> mPacketWriters;
  private boolean mClosed;

  /**
   * Creates a {@link PacketOutStream} that writes to a local file.
   *
   * @param context the file system context
   * @param address the worker network address
   * @param id the ID
   * @param length the block or file length
   * @param options the out stream options
   * @return the {@link PacketOutStream} created
   */
  public static PacketOutStream createLocalPacketOutStream(FileSystemContext context,
      WorkerNetAddress address, long id, long length, OutStreamOptions options) throws IOException {
    long packetSize = Configuration.getBytes(PropertyKey.USER_LOCAL_WRITER_PACKET_SIZE_BYTES);
    PacketWriter packetWriter =
        LocalFilePacketWriter.create(context, address, id, packetSize, options);
    return new PacketOutStream(packetWriter, length);
  }

  /**
   * Creates a {@link PacketOutStream} that writes to a netty data server.
   *
   * @param context the file system context
   * @param address the netty data server address
   * @param length the block or file length
   * @param partialRequest details of the write request which are constant for all requests
   * @param options the out stream options
   * @return the {@link PacketOutStream} created
   */
  public static PacketOutStream createNettyPacketOutStream(FileSystemContext context,
      WorkerNetAddress address, long length, Protocol.WriteRequest partialRequest,
      OutStreamOptions options) throws IOException {
    long packetSize =
        Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
    PacketWriter packetWriter =
        new NettyPacketWriter(context, address, length, partialRequest, packetSize);
    return new PacketOutStream(packetWriter, length);
  }

  // ALLUXIO CS ADD
  /**
   * Creates a {@link PacketOutStream} that writes to a list of locations.
   *
   * @param context the file system context
   * @param addresses a list of block worker addresses
   * @param length the block or file length
   * @param partialRequest details of the write request which are constant for all requests
   * @param options the out stream options
   * @return the {@link PacketOutStream} created
   */
  public static PacketOutStream createReplicatedPacketOutStream(
      FileSystemContext context, List<WorkerNetAddress> addresses, long length,
      Protocol.WriteRequest partialRequest, OutStreamOptions options) throws IOException {
    List<PacketWriter> packetWriters = new ArrayList<>();
    for (WorkerNetAddress address: addresses) {
      if (alluxio.util.CommonUtils.isLocalHost(address)) {
        long packetSize = Configuration.getBytes(PropertyKey.USER_LOCAL_WRITER_PACKET_SIZE_BYTES);
        PacketWriter packetWriter = LocalFilePacketWriter
            .create(context, address, partialRequest.getId(), packetSize, options);
        packetWriters.add(packetWriter);
      } else {
        long packetSize =
            Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
        PacketWriter packetWriter =
            new NettyPacketWriter(context, address, length, partialRequest, packetSize);
        packetWriters.add(packetWriter);
      }
    }
    return new PacketOutStream(packetWriters, length);
  }

  // ALLUXIO CS END
  /**
   * Constructs a new {@link PacketOutStream} with only one {@link PacketWriter}.
   *
   * @param packetWriter the packet writer
   * @param length the length of the stream
   */
  protected PacketOutStream(PacketWriter packetWriter, long length) {
    mCloser = Closer.create();
    mLength = length;
    mPacketWriters = new ArrayList<>(1);
    mPacketWriters.add(packetWriter);
    mCloser.register(packetWriter);
    mClosed = false;
  }

  // ALLUXIO CS ADD
  /**
   * Constructs a new {@link PacketOutStream} with multiple {@link PacketWriter}s.
   *
   * @param packetWriters the packet writers
   * @param length the length of the stream
   */
  protected PacketOutStream(List<PacketWriter> packetWriters, long length) {
    mCloser = Closer.create();
    mLength = length;
    mPacketWriters = packetWriters;
    for (PacketWriter packetWriter : packetWriters) {
      mCloser.register(packetWriter);
    }
    mClosed = false;
  }

  // ALLUXIO CS END
  /**
   * @return the remaining size of the block
   */
  @Override
  public long remaining() {
    long pos = Long.MAX_VALUE;
    for (PacketWriter packetWriter : mPacketWriters) {
      pos = Math.min(pos, packetWriter.pos());
    }
    return mLength - pos - (mCurrentPacket != null ? mCurrentPacket.readableBytes() : 0);
  }

  @Override
  public void write(int b) throws IOException {
    Preconditions.checkState(remaining() > 0, PreconditionMessage.ERR_END_OF_BLOCK);
    updateCurrentPacket(false);
    mCurrentPacket.writeByte(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return;
    }

    while (len > 0) {
      updateCurrentPacket(false);
      int toWrite = Math.min(len, mCurrentPacket.writableBytes());
      mCurrentPacket.writeBytes(b, off, toWrite);
      off += toWrite;
      len -= toWrite;
    }
    updateCurrentPacket(false);
  }

  @Override
  public void flush() throws IOException {
    if (mClosed) {
      return;
    }
    updateCurrentPacket(true);
    for (PacketWriter packetWriter : mPacketWriters) {
      packetWriter.flush();
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    releaseCurrentPacket();

    IOException exception = null;
    for (PacketWriter packetWriter : mPacketWriters) {
      try {
        packetWriter.cancel();
      } catch (IOException e) {
        if (exception != null) {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }

    close();
  }

  @Override
  public void close() throws IOException {
    try {
      updateCurrentPacket(true);
    } catch (Throwable t) {
      mCloser.rethrow(t);
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }

  /**
   * Updates the current packet.
   *
   * @param lastPacket if the current packet is the last packet
   */
  private void updateCurrentPacket(boolean lastPacket) throws IOException {
    // Early return for the most common case.
    if (mCurrentPacket != null && mCurrentPacket.writableBytes() > 0 && !lastPacket) {
      return;
    }

    if (mCurrentPacket == null) {
      if (!lastPacket) {
        mCurrentPacket = allocateBuffer();
      }
      return;
    }

    if (mCurrentPacket.writableBytes() == 0 || lastPacket) {
      try {
        if (mCurrentPacket.readableBytes() > 0) {
          for (PacketWriter packetWriter : mPacketWriters) {
            mCurrentPacket.retain();
            packetWriter.writePacket(mCurrentPacket.duplicate());
          }
        } else {
          Preconditions.checkState(lastPacket);
        }
      } finally {
        // If the packet has bytes to read, we increment its refcount explicitly for every packet
        // writer. So we need to release here. If the packet has no bytes to read, then it has
        // to be the last packet. It needs to be released as well.
        mCurrentPacket.release();
      }
      mCurrentPacket = null;
    }
    if (!lastPacket) {
      mCurrentPacket = allocateBuffer();
    }
  }

  /**
   * Releases the current packet.
   */
  private void releaseCurrentPacket() {
    if (mCurrentPacket != null) {
      mCurrentPacket.release();
      mCurrentPacket = null;
    }
  }

  /**
   * @return a newly allocated byte buffer of the user defined default size
   */
  private ByteBuf allocateBuffer() {
    return PooledByteBufAllocator.DEFAULT.buffer(mPacketWriters.get(0).packetSize());
  }
}
