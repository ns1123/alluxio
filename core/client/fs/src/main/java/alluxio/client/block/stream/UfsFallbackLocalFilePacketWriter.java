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

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A packet writer that writes to local first and fallback to UFS block writes when the block
 * storage on this local worker is full.
 */
public class UfsFallbackLocalFilePacketWriter implements PacketWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFilePacketWriter.class);
  private final LocalFilePacketWriter mLocalFilePacketWriter;
  private final FileSystemContext mContext;
  private final WorkerNetAddress mWorkerNetAddress;
  private final Protocol.WriteRequest mRequestType;
  private final long mBlockSize;
  private final long mPacketSize;
  private NettyPacketWriter mNettyPacketWriter;
  private boolean mWritingToLocal = true;


  public static UfsFallbackLocalFilePacketWriter create(FileSystemContext context,
      WorkerNetAddress address, long blockId, long blockSize, long packetSize,
      OutStreamOptions options) throws IOException {
    LocalFilePacketWriter localFilePacketWriter =
        LocalFilePacketWriter.create(context, address, blockId, options);
    return new UfsFallbackLocalFilePacketWriter(localFilePacketWriter, context, address, blockId,
        blockSize, packetSize, options);
  }

  public UfsFallbackLocalFilePacketWriter(LocalFilePacketWriter localFilePacketWriter,
      FileSystemContext context, final WorkerNetAddress address, long blockId, long blockSize,
      long packetSize, OutStreamOptions options) {
    mLocalFilePacketWriter = localFilePacketWriter;
    mContext = context;
    mWorkerNetAddress = address;
    mBlockSize = blockSize;
    mRequestType = Protocol.WriteRequest.newBuilder().setId(blockId).setTier(options.getWriteTier())
        .setType(Protocol.RequestType.UFS_BLOCK).buildPartial();
    mPacketSize = packetSize;
  }

  @Override
  public void writePacket(ByteBuf packet) throws IOException {
    if (mWritingToLocal) {
      try {
        mLocalFilePacketWriter.writePacket(packet);
      } catch (ResourceExhaustedException e) {
        LOG.warn("Not enough space, fall back to UFS");
      }
      // Close the writer to close the temp block on ramdisk
      mLocalFilePacketWriter.getWriter().close();
      mNettyPacketWriter =
          new NettyPacketWriter(mContext, mWorkerNetAddress, mBlockSize, mRequestType, mPacketSize);

      // Clean up the state of the temp block
      mLocalFilePacketWriter.cancel();
      mWritingToLocal = false;
    }
    mNettyPacketWriter.writePacket(packet);
  }

  @Override
  public void flush() throws IOException {
    if (!mWritingToLocal) {
      mLocalFilePacketWriter.flush();
    }
    mNettyPacketWriter.flush();
  }

  @Override
  public int packetSize() {
    if (!mWritingToLocal) {
      return mLocalFilePacketWriter.packetSize();
    }
    return mNettyPacketWriter.packetSize();
  }

  @Override
  public long pos() {
    if (!mWritingToLocal) {
      return mLocalFilePacketWriter.pos();
    }
    return mNettyPacketWriter.pos();
  }

  @Override
  public void cancel() throws IOException {
    if (!mWritingToLocal) {
      mLocalFilePacketWriter.cancel();
    }
    mNettyPacketWriter.cancel();
  }

  @Override
  public void close() throws IOException {
    if (!mWritingToLocal) {
      mLocalFilePacketWriter.close();
    }
    mLocalFilePacketWriter.close();
    mNettyPacketWriter.close();
  }
}
