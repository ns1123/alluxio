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
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block write request. Check more information in
 * {@link AbstractWriteHandler}.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
    value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class BlockWriteHandler extends AbstractWriteHandler<BlockWriteRequestContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWriteHandler.class);
  private static final long FILE_BUFFER_SIZE = Configuration.getBytes(
      PropertyKey.WORKER_FILE_BUFFER_SIZE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
  private final UfsManager mUfsManager;

  /**
   * Creates an instance of {@link BlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param blockWorker the block worker
   */
  BlockWriteHandler(ExecutorService executorService, BlockWorker blockWorker) {
    this(executorService, blockWorker, null);
  }

  /**
   * Creates an instance of {@link BlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param blockWorker the block worker
   * @param ufsManager the UFS manager
   */
  BlockWriteHandler(ExecutorService executorService, BlockWorker blockWorker,
      UfsManager ufsManager) {
    super(executorService);
    mWorker = blockWorker;
    mUfsManager = ufsManager;
  }

  // ALLUXIO CS ADD
  @Override
  protected void checkAccessMode(io.netty.channel.ChannelHandlerContext ctx, long blockId,
      alluxio.proto.security.CapabilityProto.Capability capability,
      alluxio.security.authorization.Mode.Bits accessMode)
      throws alluxio.exception.InvalidCapabilityException,
      alluxio.exception.AccessControlException {
    Utils.checkAccessMode(mWorker, ctx, blockId, capability, accessMode);
  }

  // ALLUXIO CS END
  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.WriteRequest request = ((RPCProtoMessage) object).getMessage().asWriteRequest();
    return request.getType() == Protocol.RequestType.ALLUXIO_BLOCK;
  }

  @Override
  protected PacketWriter createPacketWriter(BlockWriteRequestContext context, Channel channel) {
    // ALLUXIO REPLACE
    // return new BlockPacketWriter(context, channel, mWorker);
    // ALLUXIO WITH
    BlockPacketWriter writer = new BlockPacketWriter(context, channel, mWorker);
    return new UfsFallbackBlockPacketWriter(context, channel, mUfsManager, writer);
    // ALLUXIO END
  }

  @Override
  protected BlockWriteRequestContext createRequestContext(Protocol.WriteRequest msg) {
    BlockWriteRequestContext context = new BlockWriteRequestContext(msg, FILE_BUFFER_SIZE);
    return context;
  }

  @Override
  protected void initRequestContext(BlockWriteRequestContext context) throws Exception {
    BlockWriteRequest request = context.getRequest();
    mWorker.createBlockRemote(request.getSessionId(), request.getId(),
        mStorageTierAssoc.getAlias(request.getTier()), FILE_BUFFER_SIZE);
  }

  /**
   * The packet writer that writes to a local block worker.
   */
  public class BlockPacketWriter extends PacketWriter {
    /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
    private final BlockWorker mWorker;

    /**
     * @param context context of this packet writer
     * @param channel netty channel
     * @param worker local block worker
     */
    public BlockPacketWriter(
        BlockWriteRequestContext context, Channel channel, BlockWorker worker) {
      super(context, channel);
      mWorker = worker;
    }

    @Override
    protected void completeRequest(BlockWriteRequestContext context, Channel channel)
        throws Exception {
      WriteRequest request = context.getRequest();
      if (context.getBlockWriter() != null) {
        context.getBlockWriter().close();
      }
      mWorker.commitBlock(request.getSessionId(), request.getId());
    }

    @Override
    protected void cancelRequest(BlockWriteRequestContext context) throws Exception {
      WriteRequest request = context.getRequest();
      if (context.getBlockWriter() != null) {
        context.getBlockWriter().close();
      }
      mWorker.abortBlock(request.getSessionId(), request.getId());
    }

    @Override
    protected void cleanupRequest(BlockWriteRequestContext context) throws Exception {
      WriteRequest request = context.getRequest();
      mWorker.cleanupSession(request.getSessionId());
    }

    @Override
    protected void writeBuf(BlockWriteRequestContext context, Channel channel, ByteBuf buf,
        long pos) throws Exception {
      WriteRequest request = context.getRequest();
      long bytesReserved = context.getBytesReserved();
      if (bytesReserved < pos) {
        long bytesToReserve = Math.max(FILE_BUFFER_SIZE, pos - bytesReserved);
        // Allocate enough space in the existing temporary block for the write.
        mWorker.requestSpace(request.getSessionId(), request.getId(), bytesToReserve);
        context.setBytesReserved(bytesReserved + bytesToReserve);
      }
      if (context.getBlockWriter() == null) {
        String metricName = "BytesWrittenAlluxio";
        // ALLUXIO CS ADD
        String user = channel.attr(alluxio.netty.NettyAttributes.CHANNEL_KERBEROS_USER_KEY).get();
        if (user != null) {
          metricName = String.format("BytesWrittenAlluxio-User:%s", user);
        }
        // ALLUXIO CS END
        context.setBlockWriter(
            mWorker.getTempBlockWriterRemote(request.getSessionId(), request.getId()));
        context.setCounter(MetricsSystem.workerCounter(metricName));
      }
      Preconditions.checkState(context.getBlockWriter() != null);
      GatheringByteChannel outputChannel = context.getBlockWriter().getChannel();
      int sz = buf.readableBytes();
      Preconditions.checkState(buf.readBytes(outputChannel, sz) == sz);
    }
  }
  // ALLUXIO CS ADD

  /**
   * A packet writer that falls back to write the block to UFS in case the local block store is
   * full.
   */
  @NotThreadSafe
  public class UfsFallbackBlockPacketWriter extends PacketWriter {
    /** The block writer writing to the Alluxio storage of the local worker. */
    private final BlockPacketWriter mBlockPacketWriter;
    private final UfsManager mUfsManager;
    private boolean mIsWritingToLocal = true;

    /**
     * @param context context of this packet writer
     * @param channel netty channel
     * @param ufsManager UFS manager
     * @param writer local block store writer
     */
    public UfsFallbackBlockPacketWriter(
        BlockWriteRequestContext context, Channel channel, alluxio.underfs.UfsManager ufsManager,
        BlockPacketWriter writer) {
      super(context, channel);
      mBlockPacketWriter = writer;
      mUfsManager = Preconditions.checkNotNull(ufsManager);
    }

    @Override
    protected void completeRequest(BlockWriteRequestContext context, Channel channel)
        throws Exception {
      if (mIsWritingToLocal) {
        mBlockPacketWriter.completeRequest(context, channel);
      } else {
        Preconditions.checkNotNull(context.getOutputStream());
        context.getOutputStream().close();
        context.setOutputStream(null);
      }
    }

    @Override
    protected void cancelRequest(BlockWriteRequestContext context) throws Exception {
      if (mIsWritingToLocal) {
        mBlockPacketWriter.cancelRequest(context);
      } else {
        Preconditions.checkNotNull(context.getOutputStream());
        Preconditions.checkNotNull(context.getUnderFileSystem());
        context.getOutputStream().close();
        context.getUnderFileSystem().deleteFile(context.getUfsPath());
        context.setOutputStream(null);
      }
    }

    @Override
    protected void cleanupRequest(BlockWriteRequestContext context) throws Exception {
      if (mIsWritingToLocal) {
        mBlockPacketWriter.cleanupRequest(context);
      } else {
        Preconditions.checkNotNull(context.getOutputStream());
        cancelRequest(context);
      }
    }

    @Override
    protected void writeBuf(BlockWriteRequestContext context, Channel channel, ByteBuf buf,
        long pos) throws Exception {
      if (mIsWritingToLocal) {
        try {
          mBlockPacketWriter.writeBuf(context, channel, buf, pos);
          return;
        } catch (alluxio.exception.WorkerOutOfSpaceException e) {
          if (context.getRequest().hasCreateUfsBlockOptions()) {
            // we are not supposed to fall back, re-throw the error
            throw e;
          }
          // Not enough space
          LOG.warn("Not enough space to write block {} to local worker, fallback to UFS",
              context.getRequest().getId());
          mIsWritingToLocal = false;
        }
        // close the block writer first
        context.getBlockWriter().close();
        // prepare the UFS block and transfer data from the temp block to UFS
        createUfsBlock(context, channel, pos);
        // close the original block writer and remove the temp file
        mBlockPacketWriter.cancelRequest(context);
      }
      Preconditions.checkNotNull(context.getOutputStream());
      java.io.OutputStream stream = context.getOutputStream();
      buf.readBytes(stream, buf.readableBytes());
    }

    private void createUfsBlock(BlockWriteRequestContext context, Channel channel, long pos)
        throws Exception {
      BlockWriteRequest request = context.getRequest();
      Protocol.CreateUfsBlockOptions createUfsBlockOptions = request.getCreateUfsBlockOptions();
      // Before interacting with the UFS manager, make sure the user is set.
      String user = channel.attr(alluxio.netty.NettyAttributes.CHANNEL_KERBEROS_USER_KEY).get();
      if (user != null) {
        alluxio.security.authentication.AuthenticatedClientUser.set(user);
      }
      UfsManager.UfsInfo ufsInfo =
          mUfsManager.get(createUfsBlockOptions.getMountId());
      alluxio.underfs.UnderFileSystem ufs = ufsInfo.getUfs();
      context.setUnderFileSystem(ufs);
      String ufsString = MetricsSystem.escape(ufsInfo.getUfsMountPointUri());
      String ufsPath = alluxio.util.io.PathUtils.concatPath(
          ufsString, Utils.getUfsPath(mWorker.getWorkerId().get(), request.getId()));
      java.io.OutputStream ufsOutputStream =
          ufs.create(ufsPath, alluxio.underfs.options.CreateOptions.defaults());
      context.setOutputStream(ufsOutputStream);
      context.setUfsPath(ufsPath);
      String metricName;
      if (user == null) {
        metricName = String.format("BytesWrittenUfs-Ufs:%s", ufsString);
      } else {
        metricName = String.format("BytesWrittenUfs-Ufs:%s-User:%s", ufsString, user);
      }
      com.codahale.metrics.Counter counter = MetricsSystem.workerCounter(metricName);
      context.setCounter(counter);

      long sessionId = context.getRequest().getSessionId();
      long blockId = context.getRequest().getId();
      alluxio.worker.block.meta.TempBlockMeta block =
          mWorker.getBlockStore().getTempBlockMeta(sessionId, blockId);
      if (block == null) {
        throw new alluxio.exception.status.NotFoundException("block " + blockId + " not found");
      }
      io.netty.channel.FileRegion fileRegion =
          new io.netty.channel.DefaultFileRegion(new java.io.File(block.getPath()), 0, pos);
      fileRegion.transferTo(java.nio.channels.Channels.newChannel(ufsOutputStream), 0);
      fileRegion.release();
    }
  }
  // ALLUXIO CS END
}
