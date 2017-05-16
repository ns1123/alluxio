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

package alluxio.client.block;

import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.util.network.NettyUtils;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A factory class to create various block streams in Alluxio.
 */
@NotThreadSafe
public final class StreamFactory {

  private StreamFactory() {} // prevent instantiation

  /**
   * Creates an {@link BlockOutStream} that writes to a block on local worker.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the out stream options
   * @return the {@link OutputStream} object
   */
  public static BlockOutStream createLocalBlockOutStream(FileSystemContext context, long blockId,
      long blockSize, WorkerNetAddress address, OutStreamOptions options) throws IOException {
    // ALLUXIO CS ADD
    if (options.isEncrypted()) {
      blockSize = alluxio.client.LayoutUtils.toLogicalLength(
          options.getEncryptionMeta(), 0L, blockSize);
    }
    // ALLUXIO CS END
    if (NettyUtils.isDomainSocketSupported(address)) {
      return BlockOutStream
          .createNettyBlockOutStream(blockId, blockSize, address, context, options);
    }
    return BlockOutStream
        .createShortCircuitBlockOutStream(blockId, blockSize, address, context, options);
  }

  // ALLUXIO CS ADD
  /**
   * Creates an {@link OutputStream} that writes to a list of locations.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param addresses the Alluxio worker addresses
   * @param options the out stream options
   * @return the {@link OutputStream} object
   */
  public static BlockOutStream createReplicatedBlockOutStream(FileSystemContext context,
      long blockId, long blockSize, java.util.List<WorkerNetAddress> addresses,
      OutStreamOptions options) throws IOException {
    if (options.isEncrypted()) {
      blockSize = alluxio.client.LayoutUtils.toLogicalLength(
          options.getEncryptionMeta(), 0L, blockSize);
    }
    return BlockOutStream.createReplicatedBlockOutStream(blockId, blockSize, addresses, context,
        options);
  }

  // ALLUXIO CS END
  /**
   * Creates an {@link BlockOutStream} that writes to a remote worker.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the out stream options
   * @return the {@link OutputStream} object
   */
  public static BlockOutStream createRemoteBlockOutStream(FileSystemContext context, long blockId,
      long blockSize, WorkerNetAddress address, OutStreamOptions options) throws IOException {
    // ALLUXIO CS ADD
    if (options.isEncrypted()) {
      blockSize = alluxio.client.LayoutUtils.toLogicalLength(
          options.getEncryptionMeta(), 0L, blockSize);
    }
    // ALLUXIO CS END
    return BlockOutStream
        .createNettyBlockOutStream(blockId, blockSize, address, context, options);
  }

  /**
   * Creates an {@link BlockInStream} that reads from a local block.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the in stream options
   * @return the {@link InputStream} object
   */
  public static BlockInStream createLocalBlockInStream(FileSystemContext context, long blockId,
      long blockSize, WorkerNetAddress address, InStreamOptions options) throws IOException {
    // ALLUXIO CS ADD
    if (options.isEncrypted()) {
      blockSize = alluxio.client.LayoutUtils.toLogicalLength(
          options.getEncryptionMeta(), 0L, blockSize);
    }
    // ALLUXIO CS END
    if (NettyUtils.isDomainSocketSupported(address)) {
      return BlockInStream
          .createNettyBlockInStream(blockId, blockSize, address, context, options);
    }
    return BlockInStream
        .createShortCircuitBlockInStream(blockId, blockSize, address, context, options);
  }

  /**
   * Creates an {@link BlockInStream} that reads from a local block.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the in stream options
   * @return the {@link InputStream} object
   */
  public static BlockInStream createRemoteBlockInStream(FileSystemContext context, long blockId,
      long blockSize, WorkerNetAddress address, InStreamOptions options) throws IOException {
    // ALLUXIO CS ADD
    if (options.isEncrypted()) {
      blockSize = alluxio.client.LayoutUtils.toLogicalLength(
          options.getEncryptionMeta(), 0L, blockSize);
    }
    // ALLUXIO CS END
    return BlockInStream
        .createNettyBlockInStream(blockId, blockSize, address, context, options);
  }

  /**
   * Creates an {@link BlockInStream} to read a block from UFS if that block is in UFS but not in
   * Alluxio. If the block is cached to Alluxio while it attempts to create the
   * {@link BlockInStream} that reads from UFS, it returns an {@link BlockInStream} that reads from
   * Alluxio instead.
   *
   * @param context the file system context
   * @param ufsPath the UFS path
   * @param blockId the block ID
   * @param blockSize the block size
   * @param blockStart the start position of the block in the UFS file
   * @param address the worker network address
   * @param mountId the id of the mount that this file is mapped to
   * @param options the in stream options
   * @return the input stream
   */
  public static BlockInStream createUfsBlockInStream(FileSystemContext context, String ufsPath,
      long blockId, long blockSize, long blockStart, WorkerNetAddress address, long mountId,
      InStreamOptions options) throws IOException {
    return BlockInStream.createUfsBlockInStream(context, ufsPath, blockId, blockSize, blockStart,
        mountId, address, options);
  }
  // ALLUXIO CS ADD

  /**
   * Creates an {@link BlockInStream} to read the file footer.
   *
   * @param context file system context
   * @param ufsPath the ufs path
   * @param footerSize file footer size
   * @param footerPhysicalStart the physical start of the file footer
   * @param blockId the block id
   * @param address the worker address
   * @param mountId the mount id
   * @param options the options
   * @return the created {@link BlockInStream}
   */
  public static BlockInStream createFileFooterInStream(
      FileSystemContext context, String ufsPath, long footerSize, long footerPhysicalStart,
      long blockId, WorkerNetAddress address, long mountId, InStreamOptions options)
      throws IOException {
    return BlockInStream.createFileFooterInStream(
        context, ufsPath, blockId, footerSize, footerPhysicalStart, mountId, address, options);
  }
  // ALLUXIO CS END
}
