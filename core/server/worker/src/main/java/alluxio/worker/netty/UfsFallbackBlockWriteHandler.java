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
import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidCapabilityException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.status.NotFoundException;
import alluxio.metrics.MetricsSystem;
import alluxio.netty.NettyAttributes;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.security.CapabilityProto;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.meta.TempBlockMeta;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles UFS block write request. Instead of writing a block to tiered storage, this
 * handler writes the block into UFS .
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class UfsFallbackBlockWriteHandler
    extends AbstractWriteHandler<BlockWriteRequestContext> {
  private static final Logger LOG = LoggerFactory.getLogger(UfsFallbackBlockWriteHandler.class);
  private static final long FILE_BUFFER_SIZE =
      Configuration.getBytes(PropertyKey.WORKER_FILE_BUFFER_SIZE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
  private final UfsManager mUfsManager;
  private final BlockWriteHandler mBlockWriteHandler;

  /**
   * Creates an instance of {@link UfsFallbackBlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param blockWorker the block worker
   */
  UfsFallbackBlockWriteHandler(ExecutorService executorService, BlockWorker blockWorker,
      UfsManager ufsManager) {
    super(executorService);
    mWorker = blockWorker;
    mUfsManager = ufsManager;
    mBlockWriteHandler = new BlockWriteHandler(executorService, blockWorker);
  }

  @Override
  protected void checkAccessMode(ChannelHandlerContext ctx, long blockId,
      CapabilityProto.Capability capability, Mode.Bits accessMode)
      throws InvalidCapabilityException, AccessControlException {
    Utils.checkAccessMode(mWorker, ctx, blockId, capability, accessMode);
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.WriteRequest request = ((RPCProtoMessage) object).getMessage().asWriteRequest();
    return request.getType() == Protocol.RequestType.UFS_FALLBACK_BLOCK;
  }

  @Override
  protected PacketWriter createPacketWriter(BlockWriteRequestContext context, Channel channel) {
    if (context.isWritingToLocal()) {
      // not fallback yet, starting with the block packet writer
      return new UfsFallbackBlockPacketWriter(context, channel, mUfsManager,
          mBlockWriteHandler.createPacketWriter(context, channel));
    } else {
      return new UfsFallbackBlockPacketWriter(context, channel, mUfsManager, null);
    }
  }

  @Override
  protected BlockWriteRequestContext createRequestContext(Protocol.WriteRequest msg) {
    BlockWriteRequestContext context = new BlockWriteRequestContext(msg, FILE_BUFFER_SIZE);
    BlockWriteRequest request = context.getRequest();
    Preconditions.checkState(request.hasCreateUfsBlockOptions());
    // if it is already a UFS fallback from short-circuit write, avoid writing to local again
    context.setWritingToLocal(!request.getCreateUfsBlockOptions().getFallback());
    return context;
  }

  @Override
  protected void initRequestContext(BlockWriteRequestContext context) throws Exception {
    BlockWriteRequest request = context.getRequest();
    if (context.isWritingToLocal()) {
      mWorker.createBlockRemote(request.getSessionId(), request.getId(),
          mStorageTierAssoc.getAlias(request.getTier()), FILE_BUFFER_SIZE);
    }
  }

  /**
   * A packet writer that falls back to write the block to UFS in case the local block store is
   * full.
   */
  @NotThreadSafe
  public class UfsFallbackBlockPacketWriter extends PacketWriter {
    /** The block writer writing to the Alluxio storage of the local worker. */
    private final PacketWriter mBlockPacketWriter;
    private final UfsManager mUfsManager;

    /**
     * @param context context of this packet writer
     * @param channel netty channel
     * @param ufsManager UFS manager
     * @param blockPacketWriter local block store writer
     */
    public UfsFallbackBlockPacketWriter(BlockWriteRequestContext context, Channel channel,
        UfsManager ufsManager, PacketWriter blockPacketWriter) {
      super(context, channel);
      mBlockPacketWriter = blockPacketWriter;
      mUfsManager = Preconditions.checkNotNull(ufsManager);
    }

    @Override
    protected void completeRequest(BlockWriteRequestContext context, Channel channel)
        throws Exception {
      if (context.isWritingToLocal()) {
        mBlockPacketWriter.completeRequest(context, channel);
      } else {
        mWorker.commitBlockInUfs(context.getRequest().getId(), context.getPosToQueue());
        Preconditions.checkNotNull(context.getOutputStream());
        context.getOutputStream().close();
        context.setOutputStream(null);
      }
    }

    @Override
    protected void cancelRequest(BlockWriteRequestContext context) throws Exception {
      if (context.isWritingToLocal()) {
        mBlockPacketWriter.cancelRequest(context);
      } else {
        // Fixme: cancel the block in UFS
        Preconditions.checkNotNull(context.getOutputStream());
        Preconditions.checkNotNull(context.getUnderFileSystem());
        context.getOutputStream().close();
        context.getUnderFileSystem().deleteFile(context.getUfsPath());
        context.setOutputStream(null);
      }
    }

    @Override
    protected void cleanupRequest(BlockWriteRequestContext context) throws Exception {
      if (context.isWritingToLocal()) {
        mBlockPacketWriter.cleanupRequest(context);
      } else {
        Preconditions.checkNotNull(context.getOutputStream());
        cancelRequest(context);
      }
    }

    @Override
    protected void writeBuf(BlockWriteRequestContext context, Channel channel, ByteBuf buf,
        long pos) throws Exception {
      if (context.isWritingToLocal()) {
        Preconditions.checkNotNull(context.getBlockWriter());
        long posBeforeWrite = context.getBlockWriter().getPosition();
        try {
          mBlockPacketWriter.writeBuf(context, channel, buf, pos);
          return;
        } catch (WorkerOutOfSpaceException e) {
          LOG.warn("Not enough space to write block {} to local worker, fallback to UFS",
              context.getRequest().getId());
          context.setWritingToLocal(false);
        }
        // close the block writer first
        context.getBlockWriter().close();
        // prepare the UFS block and transfer data from the temp block to UFS
        createUfsBlock(context, channel);
        transferToUfsBlock(context, posBeforeWrite);
        // close the original block writer and remove the temp file
        mBlockPacketWriter.cancelRequest(context);
      }
      if (context.getOutputStream() == null) {
        createUfsBlock(context, channel);
      }
      if (buf == AbstractWriteHandler.UFS_FALLBACK_INIT) {
        // transfer data from the temp block to UFS
        transferToUfsBlock(context, pos);
      } else {
        buf.readBytes(context.getOutputStream(), buf.readableBytes());
      }
    }

    /**
     * Creates a UFS block and initialize it with bytes read from block store.
     *
     * @param context context of this request
     * @param channel netty channel
     */
    private void createUfsBlock(BlockWriteRequestContext context, Channel channel)
        throws Exception {
      BlockWriteRequest request = context.getRequest();
      Protocol.CreateUfsBlockOptions createUfsBlockOptions = request.getCreateUfsBlockOptions();
      // Before interacting with the UFS manager, make sure the user is set.
      String user = channel.attr(NettyAttributes.CHANNEL_KERBEROS_USER_KEY).get();
      if (user != null) {
        AuthenticatedClientUser.set(user);
      }
      UfsManager.UfsInfo ufsInfo = mUfsManager.get(createUfsBlockOptions.getMountId());
      UnderFileSystem ufs = ufsInfo.getUfs();
      context.setUnderFileSystem(ufs);
      String ufsString = MetricsSystem.escape(ufsInfo.getUfsMountPointUri());
      String ufsPath = Utils.getUfsBlockPath(ufsInfo, request.getId());
      // Set the atomic flag to be true to ensure only the creation of this file is atomic on close.
      OutputStream ufsOutputStream =
          ufs.create(ufsPath, CreateOptions.defaults().setEnsureAtomic(true).setCreateParent(true));
      context.setOutputStream(ufsOutputStream);
      context.setUfsPath(ufsPath);
      String metricName;
      if (user == null) {
        metricName = String.format("BytesWrittenUfs-Ufs:%s", ufsString);
      } else {
        metricName = String.format("BytesWrittenUfs-Ufs:%s-User:%s", ufsString, user);
      }
      Counter counter = MetricsSystem.workerCounter(metricName);
      context.setCounter(counter);
    }

    /**
     * Transfers data from block store to UFS.
     *
     * @param context context of this request
     * @param pos number of bytes in block store to write in the UFS block
     */
    private void transferToUfsBlock(BlockWriteRequestContext context, long pos) throws Exception {
      OutputStream ufsOutputStream = context.getOutputStream();

      long sessionId = context.getRequest().getSessionId();
      long blockId = context.getRequest().getId();
      TempBlockMeta block = mWorker.getBlockStore().getTempBlockMeta(sessionId, blockId);
      if (block == null) {
        throw new NotFoundException("block " + blockId + " not found");
      }
      FileRegion fileRegion = new DefaultFileRegion(new File(block.getPath()), 0, pos);
      fileRegion.transferTo(Channels.newChannel(ufsOutputStream), 0);
      fileRegion.release();
    }
  }
}
