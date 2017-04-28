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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.LayoutSpec;
import alluxio.client.LayoutUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.security.CryptoKey;
import alluxio.client.security.CryptoUtils;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.Status;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.LockResource;
import alluxio.util.proto.ProtoMessage;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A netty packet writer with encryption.
 * TODO(chaomin): this is just a prototype, need to dedup the logic with {@link NettyPacketWriter}.
 */
@NotThreadSafe
public final class CryptoNettyPacketWriter implements PacketWriter {
  private static final Logger LOG = LoggerFactory.getLogger(CryptoNettyPacketWriter.class);

  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS);
  private static final long WRITE_TIMEOUT_MS =
      Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
  private static final String CIPHER_NAME = "AES/GCM/NoPadding";
  private static final long CHUNK_HEADER_SIZE =
      Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES);
  private static final long CHUNK_SIZE =
      Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
  private static final long CHUNK_FOOTER_SIZE =
      Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);
  private static final int CHUNKS_PER_PACKET = (int) (PACKET_SIZE / CHUNK_SIZE);

  private final FileSystemContext mContext;
  private final Channel mChannel;
  private final InetSocketAddress mAddress;
  /* The logical length. */
  private final long mLength;
  private final Protocol.WriteRequest mPartialRequest;
  private final LayoutSpec mLayoutSpec;

  private boolean mClosed;

  private ReentrantLock mLock = new ReentrantLock();
  /** The next logical pos to write to the channel. */
  @GuardedBy("mLock")
  private long mPosToWrite;
  /**
   * The next logical pos to queue to the netty buffer. mPosToQueue - mPosToWrite is the data
   * sitting in the netty buffer.
   */
  @GuardedBy("mLock")
  private long mPosToQueue;
  /**
   * The next physical pos to queue, which is in sync with mPosToQueue.
   */
  @GuardedBy("mLock")
  private long mPhysicalPosToQueue;
  @GuardedBy("mLock")
  private Throwable mPacketWriteException;
  @GuardedBy("mLock")
  private boolean mDone;
  @GuardedBy("mLock")
  private boolean mEOFSent;
  @GuardedBy("mLock")
  private boolean mCancelSent;
  /** This condition is met if mPacketWriteException != null or mDone = true. */
  private Condition mDoneOrFailed = mLock.newCondition();
  /** This condition is met if mPacketWriteException != null or the buffer is not full. */
  private Condition mBufferNotFullOrFailed = mLock.newCondition();
  /** This condition is met if there is nothing in the netty buffer. */
  private Condition mBufferEmptyOrFailed = mLock.newCondition();

  /**
   * Creates an instance of {@link CryptoNettyPacketWriter}.
   *
   * @param context the file system context
   * @param address the data server network address
   * @param id the block ID or UFS file ID
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param sessionId the session ID
   * @param tier the target tier
   * @param type the request type (block or UFS file)
   * @throws IOException it fails to acquire a netty channel
   */
  public CryptoNettyPacketWriter(
      FileSystemContext context, final InetSocketAddress address, long id,
      long length, long sessionId, int tier, Protocol.RequestType type) throws IOException {
    this(context, address, length, Protocol.WriteRequest.newBuilder().setId(id)
        .setSessionId(sessionId).setTier(tier).setType(type).buildPartial());
  }

  /**
   * Creates an instance of {@link CryptoNettyPacketWriter}.
   *
   * @param context the file system context
   * @param address the data server network address
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param partialRequest details of the write request which are constant for all requests
   * @throws IOException it fails to acquire a netty channel
   */
  public CryptoNettyPacketWriter(
      FileSystemContext context, final InetSocketAddress address, long length,
      Protocol.WriteRequest partialRequest) throws IOException {
    Preconditions.checkState(PACKET_SIZE % CHUNK_SIZE == 0);
    Preconditions.checkState(CHUNKS_PER_PACKET >= 1);
    mLayoutSpec = new LayoutSpec(0 /* packet header */, 0 /* packet footer */, PACKET_SIZE,
        CHUNK_HEADER_SIZE, CHUNK_SIZE, CHUNK_FOOTER_SIZE);
    mContext = context;
    mAddress = address;
    mLength = length;
    mPartialRequest = partialRequest;
    mChannel = mContext.acquireNettyChannel(address);
    // TODO(peis): Move this logic to NettyClient.
    alluxio.client.netty.NettyClient.waitForChannelReady(mChannel);
    mChannel.pipeline().addLast(new PacketWriteHandler());
  }

  @Override
  public long pos() {
    try (LockResource lr = new LockResource(mLock)) {
      return mPosToQueue;
    }
  }

  @Override
  public void writePacket(final ByteBuf buf) throws IOException {
    final long len;
    final long offset;
    final long physicalOffset;
    final long physicalLen;
    try (LockResource lr = new LockResource(mLock)) {
      Preconditions.checkState(!mClosed && !mEOFSent && !mCancelSent);
      Preconditions.checkArgument(buf.readableBytes() <= PACKET_SIZE);
      while (true) {
        if (mPacketWriteException != null) {
          throw new IOException(mPacketWriteException);
        }
        if (!tooManyPacketsInFlight()) {
          offset = mPosToQueue;
          physicalOffset = mPhysicalPosToQueue;
          mPosToQueue += buf.readableBytes();
          len = buf.readableBytes();
          physicalLen = LayoutUtils.toPhysicalLength(mLayoutSpec, offset, len);
          mPhysicalPosToQueue += physicalLen;
          break;
        }
        try {
          if (!mBufferNotFullOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            throw new IOException(
                String.format("Timeout writing to %s for request %s.", mAddress, mPartialRequest));
          }
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    } catch (Throwable e) {
      buf.release();
      throw e;
    }

    int logicalPos = 0;
    int physicalPos = 0;
    byte[] plainChunk = new byte[(int) CHUNK_SIZE];
    byte[] ciphertext = new byte[(int) physicalLen];
    while (physicalPos < physicalLen) {
      // Write a full physical chunk or the last chunk to the EOF.
      int logicalChunkLen = (int) Math.min(len - logicalPos, CHUNK_SIZE);
      buf.getBytes(logicalPos, plainChunk, 0 /* dest index */, logicalChunkLen /* len */);
      CryptoKey encryptKey = new alluxio.client.security.CryptoKey(
          CIPHER_NAME, Constants.ENCRYPTION_KEY_FOR_TESTING.getBytes(),
          Constants.ENCRYPTION_IV_FOR_TESTING.getBytes(), true);
      int numBytesDone = CryptoUtils.encrypt(
          encryptKey, plainChunk, 0, logicalChunkLen, ciphertext, physicalPos);
      Preconditions.checkState(numBytesDone == logicalChunkLen + CHUNK_FOOTER_SIZE);
      logicalPos += logicalChunkLen;
      physicalPos += numBytesDone;
    }

    Protocol.WriteRequest writeRequest =
        mPartialRequest.toBuilder().setOffset(physicalOffset).build();
    DataBuffer dataBuffer = new DataNettyBufferV2(Unpooled.wrappedBuffer(ciphertext));
    // The WriteListener takes logical offset for posToWriteUncommitted.
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), dataBuffer))
        .addListener(new WriteListener(offset + len));
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    sendCancel();
  }

  @Override
  public void flush() throws IOException {
    mChannel.flush();

    try (LockResource lr = new LockResource(mLock)) {
      while (true) {
        if (mPosToWrite == mPosToQueue) {
          return;
        }
        if (mPacketWriteException != null) {
          throw new IOException(mPacketWriteException);
        }
        if (!mBufferEmptyOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new IOException(
              String.format("Timeout flushing to %s for request %s.", mAddress, mPartialRequest));
        }
      }
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    sendEof();
    mLock.lock();
    try {
      while (true) {
        if (mDone) {
          return;
        }
        try {
          if (mPacketWriteException != null) {
            mChannel.close().sync();
            throw new IOException(mPacketWriteException);
          }
          if (!mDoneOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            mChannel.close().sync();
            throw new IOException(String.format(
                "Timeout closing PacketWriter to %s for request %s.", mAddress, mPartialRequest));
          }
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    } finally {
      mLock.unlock();
      if (mChannel.isOpen()) {
        mChannel.pipeline().removeLast();
      }
      mContext.releaseNettyChannel(mAddress, mChannel);
      mClosed = true;
    }
  }

  /**
   * @return true if there are too many bytes in flight
   */
  private boolean tooManyPacketsInFlight() {
    return mPosToQueue - mPosToWrite >= MAX_PACKETS_IN_FLIGHT * PACKET_SIZE;
  }

  /**
   * Sends an EOF packet to end the write request if the stream.
   */
  private void sendEof() {
    final long physicalPos;
    try (LockResource lr = new LockResource(mLock)) {
      if (mEOFSent || mCancelSent) {
        return;
      }
      mEOFSent = true;
      physicalPos = mPhysicalPosToQueue;
    }
    // Write the EOF packet at the physical pos.
    Protocol.WriteRequest writeRequest =
        mPartialRequest.toBuilder().setOffset(physicalPos).setEof(true).build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), null))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  /**
   * Sends a CANCEL packet to end the write request if the stream.
   */
  private void sendCancel() {
    final long physicalPos;
    try (LockResource lr = new LockResource(mLock)) {
      if (mEOFSent || mCancelSent) {
        return;
      }
      mCancelSent = true;
      physicalPos = mPhysicalPosToQueue;
    }
    // Write the EOF packet at the physical pos.
    Protocol.WriteRequest writeRequest =
        mPartialRequest.toBuilder().setOffset(physicalPos).setCancel(true).build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), null))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @Override
  public int packetSize() {
    return (int) PACKET_SIZE;
  }

  /**
   * The netty handler that handles netty write response.
   */
  // TODO(chaomin): dedup this with {@link NettyPacketWriter#PacketWriterHandler}
  private final class PacketWriteHandler extends ChannelInboundHandlerAdapter {
    /**
     * Default constructor.
     */
    PacketWriteHandler() {}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      Preconditions.checkState(acceptMessage(msg), "Incorrect response type.");
      RPCProtoMessage response = (RPCProtoMessage) msg;
      Protocol.Status status = response.getMessage().<Protocol.Response>getMessage().getStatus();

      if (!Status.isOk(status) && !Status.isCancelled(status)) {
        throw new IOException(String.format("Failed to write to %s with status %s for request %s.",
            mAddress, status.toString(), mPartialRequest));
      }
      try (LockResource lr = new LockResource(mLock)) {
        mDone = true;
        mDoneOrFailed.signal();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading response from netty channel {}.",
          cause);
      try (LockResource lr = new LockResource(mLock)) {
        mPacketWriteException = cause;
        mBufferNotFullOrFailed.signal();
        mDoneOrFailed.signal();
        mBufferEmptyOrFailed.signal();
      }

      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      try (LockResource lr = new LockResource(mLock)) {
        if (mPacketWriteException == null) {
          mPacketWriteException = new IOException("Channel closed.");
        }
        mBufferNotFullOrFailed.signal();
        mDoneOrFailed.signal();
        mBufferEmptyOrFailed.signal();
      }
      ctx.fireChannelUnregistered();
    }

    /**
     * @param msg the message received
     * @return true if this message should be processed
     */
    private boolean acceptMessage(Object msg) {
      if (msg instanceof RPCProtoMessage) {
        return ((RPCProtoMessage) msg).getMessage().getType() == ProtoMessage.Type.RESPONSE;
      }
      return false;
    }
  }

  /**
   * The netty channel future listener that is called when packet write is flushed.
   */
  private final class WriteListener implements ChannelFutureListener {
    private final long mPosToWriteUncommitted;

    /**
     * @param posToWriteUncommitted the pos to commit (i.e. update mPosToWrite)
     */
    WriteListener(long posToWriteUncommitted) {
      mPosToWriteUncommitted = posToWriteUncommitted;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        future.channel().close();
      }
      boolean shouldSendEOF = false;
      try (LockResource lr = new LockResource(mLock)) {
        Preconditions.checkState(mPosToWriteUncommitted - mPosToWrite <= PACKET_SIZE,
            "Some packet is not acked.");
        Preconditions.checkState(mPosToWriteUncommitted <= mLength);
        mPosToWrite = mPosToWriteUncommitted;

        if (future.cause() != null) {
          mPacketWriteException = future.cause();
          mDoneOrFailed.signal();
          mBufferNotFullOrFailed.signal();
          mBufferEmptyOrFailed.signal();
          return;
        }
        if (mPosToWrite == mPosToQueue) {
          mBufferEmptyOrFailed.signal();
        }
        if (!tooManyPacketsInFlight()) {
          mBufferNotFullOrFailed.signal();
        }
        if (mPosToWrite == mLength) {
          shouldSendEOF = true;
        }
      }
      if (shouldSendEOF) {
        sendEof();
      }
    }
  }
}

