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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.LockResource;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NettyUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;
import alluxio.worker.block.meta.TempBlockMeta;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles UFS block write request. Instead of writing a block to tiered storage, this
 * handler writes the block into UFS .
 */
@NotThreadSafe
public final class UfsBlockWriteHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(UfsBlockWriteHandler.class);
  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS);

  /** The executor service to run the {@link UfsBlockPacketWriter}s. */
  private final ExecutorService mPacketWriterExecutor;

  /**
   * Special packets used to pass control information from the I/O thread to the packet writer
   * thread.
   * EOF: the end of file.
   * CANCEL: the write request is cancelled by the client.
   * ABORT: a non-recoverable error is detected, abort this channel.
   * LOCAL: the payload is already written previously to Alluxio tiered storage on this worker.
   */
  private static final ByteBuf EOF = Unpooled.buffer(0);
  private static final ByteBuf CANCEL = Unpooled.buffer(0);
  private static final ByteBuf ABORT = Unpooled.buffer(0);
  private static final ByteBuf LOCAL = Unpooled.buffer(0);

  private ReentrantLock mLock = new ReentrantLock();

  /**
   * This is initialized only once for a whole file or block in
   * {@link #channelRead(ChannelHandlerContext, Object)}.
   * After that, it should only be used by the packet writer thread.
   * It is safe to read those final primitive fields (e.g. mId, mSessionId) if mError is not set
   * from any thread (not such usage in the code now). It is destroyed when the write request is
   * done (complete or cancel) or an error is seen.
   *
   * Using "volatile" because we want any value change of this variable to be
   * visible across both netty and I/O threads, meanwhile no atomicity of operation is assumed;
   */
  private volatile UfsBlockWriteRequestContext mContext;

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  private final UfsManager mUfsManager;

  // FDW fallback specific ADD
  private static final String MAGIC_NUMBER = "1D91AC0E";

  /**
   * @param workerId worker ID
   * @param blockId block ID
   * @return the UFS path of a block
   */
  public static String getUfsPath(long workerId, long blockId) {
    return String.format(".alluxio_blocks_%s/%s/%s", MAGIC_NUMBER, workerId, blockId);
  }
  // FDW fallback specific END


  /**
   * Creates an instance of {@link UfsBlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link UfsBlockPacketWriter}s
   * @param blockWorker the block worker
   */
  UfsBlockWriteHandler(
      ExecutorService executorService, BlockWorker blockWorker, UfsManager ufsManager) {
    mPacketWriterExecutor = executorService;
    mWorker = blockWorker;
    mUfsManager = ufsManager;
  }

  /**
   * Checks whether this object should be processed by this handler.
   *
   * @param object the object
   * @return true if this object should be processed
   */
  private boolean acceptMessage(Object object) {
    if (!(object instanceof RPCProtoMessage)) {
      return false;
    }
    RPCProtoMessage message = (RPCProtoMessage) object;
    if (message.getType() != RPCMessage.Type.RPC_WRITE_REQUEST) {
      return false;
    }
    Protocol.WriteRequest request = message.getMessage().asWriteRequest();
    return request.getType() == Protocol.RequestType.UFS_BLOCK;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (!acceptMessage(object)) {
      ctx.fireChannelRead(object);
      return;
    }

    RPCProtoMessage msg = (RPCProtoMessage) object;
    Protocol.WriteRequest writeRequest = msg.getMessage().asWriteRequest();
    // Only initialize (open the writers) if this is the first packet in the block/file.
    if (writeRequest.getOffset() == 0) {

      // Expected state: context equals null as this handler is new for request, or the previous
      // context is not active (done / cancel / abort). Otherwise, notify the client an illegal
      // state. Note that, we reset the context before validation msg as validation may require to
      // update error in context.
      try (LockResource lr = new LockResource(mLock)) {
        Preconditions.checkState(mContext == null || !mContext.isPacketWriterActive());
        mContext = new UfsBlockWriteRequestContext(writeRequest);;
      }
    }

    // Validate the write request.
    validateWriteRequest(writeRequest, msg.getPayloadDataBuffer());

    try (LockResource lr = new LockResource(mLock)) {

      // If we have seen an error, return early and release the data. This can only
      // happen for those mis-behaving clients who first sends some invalid requests, then
      // then some random data. It can leak memory if we do not release buffers here.
      if (mContext.getError() != null) {
        if (msg.getPayloadDataBuffer() != null) {
          msg.getPayloadDataBuffer().release();
        }
        return;
      }

      ByteBuf buf;
      if (writeRequest.getEof()) {
        buf = EOF;
      } else if (writeRequest.getCancel()) {
        buf = CANCEL;
      } else if (writeRequest.getOffset() == 0) {
        // FDW fallback specific ADD
        buf = LOCAL;
        // FDW fallback specific END
      } else {
        DataBuffer dataBuffer = msg.getPayloadDataBuffer();
        Preconditions.checkState(dataBuffer != null && dataBuffer.getLength() > 0);
        Preconditions.checkState(dataBuffer.getNettyOutput() instanceof ByteBuf);
        buf = (ByteBuf) dataBuffer.getNettyOutput();
        mContext.setPosToQueue(mContext.getPosToQueue() + buf.readableBytes());
      }
      if (!mContext.isPacketWriterActive()) {
        mContext.setPacketWriterActive(true);
        mPacketWriterExecutor.submit(new UfsBlockPacketWriter(mContext, ctx.channel(), mUfsManager));
      }
      mContext.getPackets().offer(buf);
      if (tooManyPacketsInFlight()) {
        NettyUtils.disableAutoRead(ctx.channel());
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception caught {} in UfsBlockWriteHandler.", cause);
    pushAbortPacket(ctx.channel(), new Error(AlluxioStatusException.fromThrowable(cause), true));
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    pushAbortPacket(ctx.channel(), new Error(new InternalException("channel unregistered"), false));
    ctx.fireChannelUnregistered();
  }

  /**
   * @return true if there are too many packets in flight
   */
  @GuardedBy("mLock")
  private boolean tooManyPacketsInFlight() {
    return mContext.getPackets().size() >= MAX_PACKETS_IN_FLIGHT;
  }

  /**
   * Validates a block UFS write request.
   *
   * @param request the block write request
   * @throws InvalidArgumentException if the write request is invalid
   */
  //@GuardedBy("mLock")
  private void validateWriteRequest(Protocol.WriteRequest request, DataBuffer payload)
      throws InvalidArgumentException {
    // writes must be sequential
//    if (request.getOffset() != mContext.getPosToQueue()) {
//      throw new InvalidArgumentException(String.format(
//          "Offsets do not match [received: %d, expected: %d].",
//          request.getOffset(), mContext.getPosToQueue()));
//    }
    // cancel / eof message shouldn't have payload
    if (payload != null && payload.getLength() > 0 && (request.getCancel() || request.getEof())) {
      throw new InvalidArgumentException("Found data in a cancel/eof message.");
    }
  }

  /**
   * A runnable that polls from the packets queue and writes to the block worker or UFS.
   */
  protected class UfsBlockPacketWriter implements Runnable {
    private final Channel mChannel;
    private final UfsBlockWriteRequestContext mContext;
    private final UfsManager mUfsManager;

    /**
     * Creates an instance of {@link AbstractWriteHandler.PacketWriter}.
     *
     * @param context context of the request to complete
     * @param channel the netty channel
     * @param ufsManager UFS manager
     */
    UfsBlockPacketWriter(UfsBlockWriteRequestContext context, Channel channel,
        UfsManager ufsManager) {
      mContext = Preconditions.checkNotNull(context);
      mChannel = Preconditions.checkNotNull(channel);
      mUfsManager = Preconditions.checkNotNull(ufsManager);
    }

    @Override
    public void run() {
      try {
        runInternal();
      } catch (Throwable e) {
        // This should never happen.
        LOG.error("Failed to run PacketWriter.", e);
        throw e;
      }
    }

    private void runInternal() {
      boolean eof;
      boolean cancel;
      boolean abort;

      while (true) {
        ByteBuf buf;
        try (LockResource lr = new LockResource(mLock)) {
          buf = mContext.getPackets().poll();
          if (buf == null || buf == EOF || buf == CANCEL || buf == ABORT) {
            eof = buf == EOF;
            cancel = buf == CANCEL;
            // mError is checked here so that we can override EOF and CANCEL if error happens
            // after we receive EOF or CANCEL signal.
            // TODO(peis): Move to the pattern used in AbstractReadHandler to avoid
            // using special packets.
            abort = mContext.getError() != null;
            mContext.setPacketWriterActive(false);
            break;
          }
          // Release all the packets if we have encountered an error. We guarantee that no more
          // packets should be queued after we have received one of the done signals (EOF, CANCEL
          // or ABORT).
          if (mContext.getError() != null) {
            release(buf);
            continue;
          }
          if (!tooManyPacketsInFlight()) {
            NettyUtils.enableAutoRead(mChannel);
          }
        }

        // FDW fallback specific ADD
        // First part of the block is read from the local block store.
        if (buf == LOCAL) {
          long sessionId = mContext.getRequest().getSessionId();
          long blockId = mContext.getRequest().getId();
          TempBlockMeta block = mWorker.getBlockStore().getTempBlockMeta(sessionId, blockId);
          if (block == null) {
            pushAbortPacket(mChannel,
                new Error(new NotFoundException("block " + blockId + " not found"), true));
            continue;
          }
          try (BlockReader reader = new LocalFileBlockReader(block.getPath())) {
            ByteBuffer fileBuffer = reader.read(0, reader.getLength());
            buf = Unpooled.wrappedBuffer(fileBuffer);
            int readableBytes = buf.readableBytes();
            writeBuf(mChannel, buf, mContext.getPosToWrite());
            incrementMetrics(readableBytes);
          } catch (IOException e) {
            pushAbortPacket(mChannel,
                new Error(AlluxioStatusException.fromIOException(e), true));
          } catch (Exception e) {
            LOG.warn("Failed to write packet {}", e.getMessage());
            Throwables.propagateIfPossible(e);
            pushAbortPacket(mChannel,
                new Error(AlluxioStatusException.fromCheckedException(e), true));
          } finally {
            release(buf);
          }
          continue;
        }
        // FDW fallback specific END

        try {
          int readableBytes = buf.readableBytes();
          mContext.setPosToWrite(mContext.getPosToWrite() + readableBytes);
          writeBuf(mChannel, buf, mContext.getPosToWrite());
          incrementMetrics(readableBytes);
        } catch (Exception e) {
          LOG.warn("Failed to write packet {}", e.getMessage());
          Throwables.propagateIfPossible(e);
          pushAbortPacket(mChannel,
              new Error(AlluxioStatusException.fromCheckedException(e), true));
        } finally {
          release(buf);
        }
      }

      if (abort) {
        try {
          cleanupRequest();
          replyError();
        } catch (Exception e) {
          LOG.warn("Failed to cleanup states with error {}.", e.getMessage());
        } finally {
          reset();
        }
      } else if (cancel || eof) {
        try {
          if (cancel) {
            cancelRequest();
            replyCancel();
          } else {
            completeRequest(mChannel);
            replySuccess();
          }
        } catch (Exception e) {
          Throwables.propagateIfPossible(e);
          pushAbortPacket(mChannel,
              new Error(AlluxioStatusException.fromCheckedException(e), true));
        } finally {
          reset();
        }
      }
    }

    /**
     * Completes this write. This is called when the write completes.
     *
     * @param channel netty channel
     */
    private void completeRequest(Channel channel) throws Exception {
      if (mContext.getOutputStream() == null) {
        createUfsFile(channel);
      }
      Preconditions.checkState(mContext.getOutputStream() != null);
      mContext.getOutputStream().close();
      mContext.setOutputStream(null);
    }

    /**
     * Cancels this write. This is called when the client issues a cancel request.
     */
    private void cancelRequest() throws Exception {
      // TODO(calvin): Consider adding cancel to the ufs stream api.
      if (mContext.getOutputStream() != null && mContext.getUnderFileSystem() != null) {
        mContext.getOutputStream().close();
        mContext.getUnderFileSystem().deleteFile(mContext.getUfsPath());
        mContext.setOutputStream(null);
      }
    }

    /**
     * Cleans up this write. This is called when the write request is aborted due to any exception
     * or session timeout.
     */
    private void cleanupRequest() throws Exception {
      cancelRequest();
    }

    /**
     * Writes the buffer.
     *
     * @param channel the netty channel
     * @param buf the buffer
     * @param pos the pos
     */
    private void writeBuf(Channel channel, ByteBuf buf, long pos) throws Exception {
      if (mContext.getOutputStream() == null) {
        createUfsFile(channel);
      }

      buf.readBytes(mContext.getOutputStream(), buf.readableBytes());
    }

    private void createUfsFile(Channel channel) throws IOException {
      UfsBlockWriteRequest request = mContext.getRequest();
      Protocol.CreateUfsFileOptions createUfsFileOptions = request.getCreateUfsFileOptions();
      // Before interacting with the UFS manager, make sure the user is set.
      String user = channel.attr(alluxio.netty.NettyAttributes.CHANNEL_KERBEROS_USER_KEY).get();
      if (user != null) {
        alluxio.security.authentication.AuthenticatedClientUser.set(user);
      }
      UfsManager.UfsInfo ufsInfo = mUfsManager.get(createUfsFileOptions.getMountId());
      UnderFileSystem ufs = ufsInfo.getUfs();
      mContext.setUnderFileSystem(ufs);
      String ufsString = MetricsSystem.escape(ufsInfo.getUfsMountPointUri());
      String ufsPath = PathUtils.concatPath(
          ufsString, getUfsPath(mWorker.getWorkerId().get(), request.getId()));
      mContext.setOutputStream(ufs.create(ufsPath,
          CreateOptions.defaults().setOwner(createUfsFileOptions.getOwner())
              .setGroup(createUfsFileOptions.getGroup())
              .setMode(new Mode((short) createUfsFileOptions.getMode()))));
      mContext.setUfsPath(ufsPath);
      String metricName;
      if (user == null) {
        metricName = String.format("BytesWrittenUfs-Ufs:%s", ufsString);
      } else {
        metricName = String.format("BytesWrittenUfs-Ufs:%s-User:%s", ufsString, user);
      }
      Counter counter = MetricsSystem.workerCounter(metricName);
      mContext.setCounter(counter);
    }

    /**
     * Writes a response to signify the success of the write request.
     */
    private void replySuccess() {
      NettyUtils.enableAutoRead(mChannel);
      mChannel.writeAndFlush(RPCProtoMessage.createOkResponse(null))
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    /**
     * Writes a response to signify the successful cancellation of the write request.
     */
    private void replyCancel() {
      NettyUtils.enableAutoRead(mChannel);
      mChannel.writeAndFlush(RPCProtoMessage.createCancelResponse())
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    /**
     * Writes an error response to the channel and closes the channel after that.
     */
    private void replyError() {
      Error error;
      try (LockResource lr = new LockResource(mLock)) {
        error = Preconditions.checkNotNull(mContext.getError());
      }

      if (error.isNotifyClient()) {
        mChannel.writeAndFlush(RPCProtoMessage.createResponse(error.getCause()))
            .addListener(ChannelFutureListener.CLOSE);
      }
    }
  }

  /**
   * Resets all the states.
   */
  private void reset() {
    try (LockResource lr = new LockResource(mLock)) {
      mContext = null;
    }
  }

  /**
   * Pushes {@link #ABORT} to the buffer if there has been no error so far.
   *
   * @param channel the channel
   * @param error the error
   */
  private void pushAbortPacket(Channel channel, Error error) {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null) {
        // Note, network errors may be bubbling up through channelUnregistered to reach here before
        // mContext is initialized.
        return;
      }
      mContext.setError(error);
      mContext.getPackets().offer(ABORT);
      if (!mContext.isPacketWriterActive()) {
        mContext.setPacketWriterActive(true);
        mPacketWriterExecutor.submit(new UfsBlockPacketWriter(mContext, channel, mUfsManager));
      }
    }
  }

  /**
   * Releases a {@link ByteBuf}.
   *
   * @param buf the netty byte buffer
   */
  private static void release(ByteBuf buf) {
    if (buf != null && buf != EOF && buf != CANCEL && buf != ABORT && buf != LOCAL) {
      buf.release();
    }
  }

  /**
   * @param bytesWritten bytes written
   */
  private void incrementMetrics(long bytesWritten) {
    Counter counter = mContext.getCounter();
    Preconditions.checkState(counter != null);
    counter.inc(bytesWritten);
  }
}
