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

package alluxio.client.file;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.Seekable;
import alluxio.annotation.PublicApi;
import alluxio.client.BoundedStream;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.network.netty.NettyRPC;
import alluxio.network.netty.NettyRPCContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 *
 * This class wraps the block in stream for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 *
 * The internal bookkeeping works as follows:
 *
 * 1. {@link #updateStream()} is a potentially expensive operation and is responsible for
 * creating new BlockInStreams and updating {@link #mBlockInStream}. After calling this method,
 * {@link #mBlockInStream} is ready to serve reads from the current {@link #mPosition}.
 * 2. {@link #mPosition} can become out of sync with {@link #mBlockInStream} when seek or skip is
 * called. When this happens, {@link #mBlockInStream} is set to null and no effort is made to
 * sync between the two until {@link #updateStream()} is called.
 * 3. {@link #updateStream()} is only called when followed by a read request. Thus, if a
 * {@link #mBlockInStream} is created, it is guaranteed we read at least one byte from it.
 */
@PublicApi
@NotThreadSafe
public class FileInStream extends InputStream implements BoundedStream, PositionedReadable,
    Seekable {
  private static final Logger LOG = LoggerFactory.getLogger(FileInStream.class);

  private final URIStatus mStatus;
  private final InStreamOptions mOptions;
  private final AlluxioBlockStore mBlockStore;
  private final FileSystemContext mContext;

  /* Convenience values derived from mStatus, use these instead of querying mStatus. */
  /** Length of the file in bytes. */
  private final long mLength;
  /** Block size in bytes. */
  private final long mBlockSize;

  /* Underlying stream and associated bookkeeping. */
  /** Current offset in the file. */
  private long mPosition;
  /** Underlying block stream, null if a position change has invalidated the previous stream. */
  private BlockInStream mBlockInStream;

  protected FileInStream(URIStatus status, InStreamOptions options, FileSystemContext context) {
    mStatus = status;
<<<<<<< HEAD
    mPositionState = new PositionState(mStatus.getLength(), mStatus.getBlockIds(),
        mStatus.getBlockSizeBytes());
    mInStreamOptions = options;
    mOutStreamOptions = OutStreamOptions.defaults();
    // ALLUXIO CS ADD
    // Need to explicitly set the capability in outStreamOptions when caching passively.
    if (mStatus.getCapability() != null) {
      mOutStreamOptions.setCapabilityFetcher(new alluxio.client.security.CapabilityFetcher(
          context, mStatus.getPath(), mStatus.getCapability()));
    }
    // Inherit the encryption metadata from InStreamOptions if the file is encrypted.
    mOutStreamOptions.setEncrypted(options.isEncrypted());
    if (options.isEncrypted()) {
      mOutStreamOptions.setEncryptionMeta(options.getEncryptionMeta());
    }
    // ALLUXIO CS END
    mBlockSize = status.getBlockSizeBytes();
    mFileLength = status.getLength();
    mContext = context;
    mAlluxioStorageType = options.getAlluxioStorageType();
    mShouldCache = mAlluxioStorageType.isStore();
    mCachePartiallyReadBlock = options.isCachePartiallyReadBlock();
    mClosed = false;
    if (mShouldCache) {
      Preconditions.checkNotNull(options.getCacheLocationPolicy(),
          PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED);
    }

    int seekBufferSizeBytes = Math.max((int) options.getSeekBufferSizeBytes(), 1);
    mSeekBuffer = new byte[seekBufferSizeBytes];
||||||| merged common ancestors
    mPositionState = new PositionState(mStatus.getLength(), mStatus.getBlockIds(),
        mStatus.getBlockSizeBytes());
    mInStreamOptions = options;
    mOutStreamOptions = OutStreamOptions.defaults();
    mBlockSize = status.getBlockSizeBytes();
    mFileLength = status.getLength();
    mContext = context;
    mAlluxioStorageType = options.getAlluxioStorageType();
    mShouldCache = mAlluxioStorageType.isStore();
    mCachePartiallyReadBlock = options.isCachePartiallyReadBlock();
    mClosed = false;
    if (mShouldCache) {
      Preconditions.checkNotNull(options.getCacheLocationPolicy(),
          PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED);
    }

    int seekBufferSizeBytes = Math.max((int) options.getSeekBufferSizeBytes(), 1);
    mSeekBuffer = new byte[seekBufferSizeBytes];
=======
    mOptions = options;
>>>>>>> 1a2e8078327a0651716e3313a4a085de4ff40ded
    mBlockStore = AlluxioBlockStore.create(context);
    mContext = context;

    mLength = mStatus.getLength();
    mBlockSize = mStatus.getBlockSizeBytes();

    mPosition = 0;
    mBlockInStream = null;
  }

  /* Input Stream methods */
  @Override
  public int read() throws IOException {
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    updateStream();
    int result = mBlockInStream.read();
    if (result != -1) {
      mPosition++;
    }
    return result;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
<<<<<<< HEAD
    return readInternal(b, off, len);
  }

  private int readInternal() throws IOException {
    if (mPositionState.isEOF()) {
      return EOF_DATA;
    }
    updateStreams();
    Preconditions.checkState(mCurrentBlockInStream != null, PreconditionMessage.ERR_UNEXPECTED_EOF);

    int data = mCurrentBlockInStream.read();
    if (data == EOF_DATA) {
      // The underlying stream is done.
      return EOF_DATA;
    }

    mPositionState.increment(1);
    if (mCurrentCacheStream != null) {
      try {
        mCurrentCacheStream.write(data);
      } catch (IOException e) {
        handleCacheStreamException(e);
      }
    }
    return data;
  }

  // ALLUXIO CS REPLACE
  // private int readInternal(byte[] b, int off, int len) throws IOException {
  // ALLUXIO CS WITH
  protected int readInternal(byte[] b, int off, int len) throws IOException {
    // ALLUXIO CS END
||||||| merged common ancestors
    return readInternal(b, off, len);
  }

  private int readInternal() throws IOException {
    if (mPositionState.isEOF()) {
      return EOF_DATA;
    }
    updateStreams();
    Preconditions.checkState(mCurrentBlockInStream != null, PreconditionMessage.ERR_UNEXPECTED_EOF);

    int data = mCurrentBlockInStream.read();
    if (data == EOF_DATA) {
      // The underlying stream is done.
      return EOF_DATA;
    }

    mPositionState.increment(1);
    if (mCurrentCacheStream != null) {
      try {
        mCurrentCacheStream.write(data);
      } catch (IOException e) {
        handleCacheStreamException(e);
      }
    }
    return data;
  }

  private int readInternal(byte[] b, int off, int len) throws IOException {
=======
>>>>>>> 1a2e8078327a0651716e3313a4a085de4ff40ded
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    }
    if (mPosition == mLength) { // at end of file
      return -1;
    }

    int bytesLeft = len;
    int currentOffset = off;
    while (bytesLeft > 0 && mPosition != mLength) {
      updateStream();
      int bytesRead = mBlockInStream.read(b, currentOffset, bytesLeft);
      if (bytesRead > 0) {
        bytesLeft -= bytesRead;
        currentOffset += bytesRead;
        mPosition += bytesRead;
      }
    }
    return len - bytesLeft;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, mLength - mPosition);
    seek(mPosition + toSkip);
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    closeBlockInStream(mBlockInStream);
  }

  /* Bounded Stream methods */
  @Override
  public long remaining() {
    return mLength - mPosition;
  }

  /* Positioned Readable methods */
  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return positionedReadInternal(pos, b, off, len);
  }

  private int positionedReadInternal(long pos, byte[] b, int off, int len) throws IOException {
<<<<<<< HEAD
    if (pos < 0 || pos >= mFileLength) {
      return EOF_DATA;
    }

    // If partial read cache is enabled, we fall back to the normal read.
    if (shouldCachePartiallyReadBlock()) {
      synchronized (this) {
        long oldPos = mPositionState.getPos();
        try {
          seek(pos);
          // ALLUXIO CS REPLACE
          // return read(b, off, len);
          // ALLUXIO CS WITH
          return readInternal(b, off, len);
          // ALLUXIO CS END
        } finally {
          seek(oldPos);
        }
      }
||||||| merged common ancestors
    if (pos < 0 || pos >= mFileLength) {
      return EOF_DATA;
    }

    // If partial read cache is enabled, we fall back to the normal read.
    if (shouldCachePartiallyReadBlock()) {
      synchronized (this) {
        long oldPos = mPositionState.getPos();
        try {
          seek(pos);
          return read(b, off, len);
        } finally {
          seek(oldPos);
        }
      }
=======
    if (pos < 0 || pos >= mLength) {
      return -1;
>>>>>>> 1a2e8078327a0651716e3313a4a085de4ff40ded
    }

    int lenCopy = len;
    while (len > 0) {
      if (pos >= mLength) {
        break;
      }
      long blockId = mStatus.getBlockIds().get(Math.toIntExact(pos / mBlockSize));
      BlockInStream stream = null;
      try {
        stream = mBlockStore.getInStream(mOptions.getBlockInfo(blockId), mOptions);
        long offset = pos % mBlockSize;
        int bytesRead =
            stream.positionedRead(offset, b, off, (int) Math.min(mBlockSize - offset, len));
        Preconditions.checkState(bytesRead > 0, "No data is read before EOF");
        pos += bytesRead;
        off += bytesRead;
        len -= bytesRead;
      } finally {
        closeBlockInStream(stream);
      }
    }
    return lenCopy - len;
  }

  /* Seekable methods */
  @Override
  public long getPos() {
    return mPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mPosition == pos) {
      return;
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);

    if (mBlockInStream == null) { // no current stream open, advance position
      mPosition = pos;
      return;
    }

<<<<<<< HEAD
  /**
   * Updates the streams to be able to start reading from the current file position.
   * If the current file position is still in the range of the current stream, and the current
   * stream still has remaining data, then this is a no-op;
   * otherwise, both the block stream and the cache stream are updated.
   */
  private void updateStreams() throws IOException {
    long blockId = mPositionState.getBlockId();
    if (mCurrentBlockInStream != null && mCurrentBlockInStream.getId() == blockId
        && mCurrentBlockInStream.remaining() != 0) {
      return;
    }
    checkCacheStreamInSync();
    updateBlockInStream(blockId);
    // ALLUXIO CS REPLACE
    // if (PASSIVE_CACHE_ENABLED) {
    // ALLUXIO CS WITH
    boolean overReplicated = mStatus.getReplicationMax() > 0
        && mStatus.getFileBlockInfos().get((int) (mPositionState.getPos() / mBlockSize))
        .getBlockInfo().getLocations().size() >= mStatus.getReplicationMax();
    if (PASSIVE_CACHE_ENABLED && !overReplicated) {
    // ALLUXIO CS END
      updateCacheStream(blockId);
||||||| merged common ancestors
  /**
   * Updates the streams to be able to start reading from the current file position.
   * If the current file position is still in the range of the current stream, and the current
   * stream still has remaining data, then this is a no-op;
   * otherwise, both the block stream and the cache stream are updated.
   */
  private void updateStreams() throws IOException {
    long blockId = mPositionState.getBlockId();
    if (mCurrentBlockInStream != null && mCurrentBlockInStream.getId() == blockId
        && mCurrentBlockInStream.remaining() != 0) {
      return;
    }
    checkCacheStreamInSync();
    updateBlockInStream(blockId);
    if (PASSIVE_CACHE_ENABLED) {
      updateCacheStream(blockId);
=======
    long delta = pos - mPosition;
    if (delta <= mBlockInStream.remaining() && delta >= -mBlockInStream.getPos()) { // within block
      mBlockInStream.seek(mBlockInStream.getPos() + delta);
    } else { // close the underlying stream as the new position is no longer in bounds
      closeBlockInStream(mBlockInStream);
>>>>>>> 1a2e8078327a0651716e3313a4a085de4ff40ded
    }
    mPosition += delta;
  }

  /**
   * Initializes the underlying block stream if necessary. This method must be called before
   * reading from mBlockInStream.
   */
  private void updateStream() throws IOException {
    if (mBlockInStream != null && mBlockInStream.remaining() > 0) { // can still read from stream
      return;
    }

    if (mBlockInStream != null && mBlockInStream.remaining() == 0) { // current stream is done
      closeBlockInStream(mBlockInStream);
    }

<<<<<<< HEAD
    // Unlike updateBlockInStream below, we never start a block cache stream if mPos is in the
    // middle of a block.
    if (mPositionState.getPos() % mBlockSize != 0) {
      return;
    }

    try {
      // If this block is read from a remote worker, we should never cache except to a local worker.
      WorkerNetAddress localWorker = mContext.getLocalWorker();
      if (localWorker != null) {
        mCurrentCacheStream =
            mBlockStore.getOutStream(blockId, getBlockSize(mPositionState.getPos()), localWorker,
                mOutStreamOptions);
      }
    } catch (IOException e) {
      handleCacheStreamException(e);
    }
  }

  /**
   * Update {@link #mCurrentBlockInStream} to be in-sync with the current position.
   * This function is only called in {@link #updateStreams()}.
   *
   * @param blockId the block ID
   */
  private void updateBlockInStream(long blockId) throws IOException {
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
      mCurrentBlockInStream = null;
    }

    if (blockId == PositionState.EOF_BLOCK_ID) {
      return;
    }
    mCurrentBlockInStream = getBlockInStream(blockId);
  }

  /**
   * Gets the block in stream corresponding a block ID.
   *
   * @param blockId the block ID
   * @return the block in stream
   */
  private BlockInStream getBlockInStream(long blockId) throws IOException {
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = null;
    boolean readFromUfs = mStatus.isPersisted();
    // ALLUXIO CS ADD
    // In case it is possible to fallback to read UFS blocks, also fill in the options.
    boolean storedAsUfsBlock
        = mStatus.getPersistenceState().equals("TO_BE_PERSISTED");
    readFromUfs = readFromUfs || storedAsUfsBlock;
    // ALLUXIO CS END
    if (readFromUfs) {
      long blockStart = BlockId.getSequenceNumber(blockId) * mBlockSize;
      openUfsBlockOptions =
          Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(mStatus.getUfsPath())
              .setOffsetInFile(blockStart).setBlockSize(getBlockSize(blockStart))
              .setMaxUfsReadConcurrency(mInStreamOptions.getMaxUfsReadConcurrency())
              .setNoCache(!mInStreamOptions.getAlluxioStorageType().isStore())
              .setMountId(mStatus.getMountId()).build();
    }
    // ALLUXIO CS ADD
    // On client-side, we do not have enough mount information to fill in the UFS file path.
    // Instead, we unset the ufsPath field and fill in a flag ufsBlock to indicate the UFS file
    // path can be derived from mount id and the block ID. Also because the entire file is only
    // one block, we set the offset in file to be zero.
    if (storedAsUfsBlock) {
      openUfsBlockOptions = openUfsBlockOptions.toBuilder().clearUfsPath().setBlockInUfsTier(true)
          .setOffsetInFile(0).build();
    }
    // ALLUXIO CS END
    return mBlockStore.getInStream(blockId, openUfsBlockOptions, mInStreamOptions);
  }

  /**
   * Seeks to a file position. Blocks are not cached unless they are fully read. This is only called
   * by {@link FileInStream#seek}.
   *
   * @param pos The position to seek to. It is guaranteed to be valid (pos >= 0 && pos != mPos &&
   *        pos <= mFileLength)
   */
  private void seekInternal(long pos) throws IOException {
    closeOrCancelCacheStream();
    mPositionState.setPos(pos);
    updateStreams();
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.seek(mPositionState.getPos() % mBlockSize);
    } else {
      Preconditions.checkState(mPositionState.isEOF());
    }
||||||| merged common ancestors
    // Unlike updateBlockInStream below, we never start a block cache stream if mPos is in the
    // middle of a block.
    if (mPositionState.getPos() % mBlockSize != 0) {
      return;
    }

    try {
      // If this block is read from a remote worker, we should never cache except to a local worker.
      WorkerNetAddress localWorker = mContext.getLocalWorker();
      if (localWorker != null) {
        mCurrentCacheStream =
            mBlockStore.getOutStream(blockId, getBlockSize(mPositionState.getPos()), localWorker,
                mOutStreamOptions);
      }
    } catch (IOException e) {
      handleCacheStreamException(e);
    }
  }

  /**
   * Update {@link #mCurrentBlockInStream} to be in-sync with the current position.
   * This function is only called in {@link #updateStreams()}.
   *
   * @param blockId the block ID
   */
  private void updateBlockInStream(long blockId) throws IOException {
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
      mCurrentBlockInStream = null;
    }

    if (blockId == PositionState.EOF_BLOCK_ID) {
      return;
    }
    mCurrentBlockInStream = getBlockInStream(blockId);
  }

  /**
   * Gets the block in stream corresponding a block ID.
   *
   * @param blockId the block ID
   * @return the block in stream
   */
  private BlockInStream getBlockInStream(long blockId) throws IOException {
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = null;
    boolean readFromUfs = mStatus.isPersisted();
    if (readFromUfs) {
      long blockStart = BlockId.getSequenceNumber(blockId) * mBlockSize;
      openUfsBlockOptions =
          Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(mStatus.getUfsPath())
              .setOffsetInFile(blockStart).setBlockSize(getBlockSize(blockStart))
              .setMaxUfsReadConcurrency(mInStreamOptions.getMaxUfsReadConcurrency())
              .setNoCache(!mInStreamOptions.getAlluxioStorageType().isStore())
              .setMountId(mStatus.getMountId()).build();
    }
    return mBlockStore.getInStream(blockId, openUfsBlockOptions, mInStreamOptions);
  }

  /**
   * Seeks to a file position. Blocks are not cached unless they are fully read. This is only called
   * by {@link FileInStream#seek}.
   *
   * @param pos The position to seek to. It is guaranteed to be valid (pos >= 0 && pos != mPos &&
   *        pos <= mFileLength)
   */
  private void seekInternal(long pos) throws IOException {
    closeOrCancelCacheStream();
    mPositionState.setPos(pos);
    updateStreams();
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.seek(mPositionState.getPos() % mBlockSize);
    } else {
      Preconditions.checkState(mPositionState.isEOF());
    }
=======
    /* Create a new stream to read from mPosition. */
    // Calculate block id.
    long blockId = mStatus.getBlockIds().get(Math.toIntExact(mPosition / mBlockSize));
    // Create stream
    mBlockInStream = mBlockStore.getInStream(mOptions.getBlockInfo(blockId), mOptions);
    // Set the stream to the correct position.
    long offset = mPosition % mBlockSize;
    mBlockInStream.seek(offset);
>>>>>>> 1a2e8078327a0651716e3313a4a085de4ff40ded
  }

  private void closeBlockInStream(BlockInStream stream) throws IOException {
    if (stream != null) {
      // Get relevant information from the stream.
      WorkerNetAddress dataSource = stream.getAddress();
      long blockId = stream.getId();
      BlockInStream.BlockInStreamSource blockSource = stream.getSource();
      stream.close();
      // TODO(calvin): we should be able to do a close check instead of using null
      if (stream == mBlockInStream) { // if stream is instance variable, set to null
        mBlockInStream = null;
      }
      if (blockSource == BlockInStream.BlockInStreamSource.LOCAL) {
        return;
      }

      // Send an async cache request to a worker based on read type and passive cache options.
      boolean cache = mOptions.getOptions().getReadType().isCache();
      boolean passiveCache = Configuration.getBoolean(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED);
      long channelTimeout = Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
      if (cache) {
        WorkerNetAddress worker;
        if (passiveCache && mContext.hasLocalWorker()) { // send request to local worker
          worker = mContext.getLocalWorker();
        } else { // send request to data source
          worker = dataSource;
        }
        try {
          // Construct the async cache request
          Protocol.AsyncCacheRequest request =
              Protocol.AsyncCacheRequest.newBuilder().setBlockId(blockId)
                  .setOpenUfsBlockOptions(mOptions.getOpenUfsBlockOptions(blockId))
                  .setSourceHost(dataSource.getHost()).setSourcePort(dataSource.getDataPort())
                  .build();
          Channel channel = mContext.acquireNettyChannel(worker);
          try {
            NettyRPCContext rpcContext =
                NettyRPCContext.defaults().setChannel(channel).setTimeout(channelTimeout);
            NettyRPC.call(rpcContext, new ProtoMessage(request));
          } finally {
            mContext.releaseNettyChannel(worker, channel);
          }
        } catch (Exception e) {
          LOG.warn("Failed to complete async cache request for block {} at worker {}: {}", blockId,
              worker, e.getMessage());
        }
      }
    }
  }
}
