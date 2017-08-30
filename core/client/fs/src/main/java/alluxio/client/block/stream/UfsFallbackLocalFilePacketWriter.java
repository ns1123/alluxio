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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A packet writer that writes to local first and fallback to UFS block writes when the block
 * storage on this local worker is full.
 */
@NotThreadSafe
public final class UfsFallbackLocalFilePacketWriter implements PacketWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFilePacketWriter.class);
  private final LocalFilePacketWriter mLocalFilePacketWriter;
  private final FileSystemContext mContext;
  private final WorkerNetAddress mWorkerNetAddress;
  private final long mBlockSize;
  private final long mBlockId;
  private final OutStreamOptions mOutStreamOptions;
  private NettyPacketWriter mNettyPacketWriter;
  private boolean mIsWritingToLocal = true;

  /**
   * @param context the file system context
   * @param address the worker network address
   * @param blockId the block ID
   * @param blockSize the block size
   * @param options the output stream options
   * @return the {@link UfsFallbackLocalFilePacketWriter} instance created
   */
  public static UfsFallbackLocalFilePacketWriter create(FileSystemContext context,
      WorkerNetAddress address, long blockId, long blockSize, OutStreamOptions options)
      throws IOException {
    LocalFilePacketWriter localFilePacketWriter =
        LocalFilePacketWriter.create(context, address, blockId, options);
    return new UfsFallbackLocalFilePacketWriter(localFilePacketWriter, context, address, blockId,
        blockSize, options);
  }

  private UfsFallbackLocalFilePacketWriter(LocalFilePacketWriter localFilePacketWriter,
      FileSystemContext context, final WorkerNetAddress address, long blockId, long blockSize,
      OutStreamOptions options) {
    mLocalFilePacketWriter = localFilePacketWriter;
    mBlockId = blockId;
    mContext = context;
    mWorkerNetAddress = address;
    mBlockSize = blockSize;
    mOutStreamOptions = options;
  }

  @Override
  public void writePacket(ByteBuf packet) throws IOException {
    if (mIsWritingToLocal) {
      long pos = mLocalFilePacketWriter.pos();
      try {
        packet.retain(); // increase the refcount by 1
        mLocalFilePacketWriter.writePacket(packet); // this will release packet by 1 regardless
        return;
      } catch (ResourceExhaustedException e) {
        LOG.warn("Not enough space on local worker, fallback to write block {} to UFS", mBlockId);
        mIsWritingToLocal = false;
      }
      try {
        // Close the writer to close the temp block on ramdisk
        mLocalFilePacketWriter.flush();
        mLocalFilePacketWriter.getWriter().close();
        mNettyPacketWriter = NettyPacketWriter
            .create(mContext, mWorkerNetAddress, mBlockId, mBlockSize,
                Protocol.RequestType.UFS_BLOCK, mOutStreamOptions.setSizeWritten(pos));
        // Clean up the state of the temp block
        mLocalFilePacketWriter.cancel();
      } catch (Exception e) {
        throw new IOException("Failed to switch to writing to UFS", e);
      }
    }
    mNettyPacketWriter.writePacket(packet);
  }

  @Override
  public void flush() throws IOException {
    if (mIsWritingToLocal) {
      mLocalFilePacketWriter.flush();
    } else {
      mNettyPacketWriter.flush();
    }
  }

  @Override
  public int packetSize() {
    if (mIsWritingToLocal) {
      return mLocalFilePacketWriter.packetSize();
    } else {
      return mNettyPacketWriter.packetSize();
    }
  }

  @Override
  public long pos() {
    if (mIsWritingToLocal) {
      return mLocalFilePacketWriter.pos();
    } else {
      return mNettyPacketWriter.pos();
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mIsWritingToLocal) {
      mLocalFilePacketWriter.cancel();
    } else {
      mNettyPacketWriter.cancel();
    }
  }

  @Override
  public void close() throws IOException {
    if (mIsWritingToLocal) {
      mLocalFilePacketWriter.close();
    } else {
      mNettyPacketWriter.close();
    }
  }
}
