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

import alluxio.Constants;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * A class that wraps multiple underlying local or remote BlockOutStreams. Any data written to this
 * BlockOutStream will be replicated and output to all the underlying streams.
 */
// TODO(binfan): currently this is a straightforward wrapper on top of multiple other
// BlockOutStreams where each stream keeps its own buffer. Refactor the BlockOutStream classes to
// have only one buffer.
public final class ReplicatedBlockOutStream extends BufferedBlockOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final List<BufferedBlockOutStream> mBlockOutStreams;

  public ReplicatedBlockOutStream(long blockId, long blockSize, BlockStoreContext blockStoreContext,
      Iterable<? extends BufferedBlockOutStream> outStreams) {
    super(blockId, blockSize, blockStoreContext);
    mBlockOutStreams = Lists.newArrayList(outStreams);
    Preconditions
        .checkArgument(mBlockOutStreams.size() > 1, "At least two BlockOutStreams required");
  }

  @Override
  protected void unBufferedWrite(byte[] b, int off, int len) throws IOException {
    for (BufferedBlockOutStream outStream : mBlockOutStreams) {
      // NOTE, we could not have outStream.unBufferedWrite() here. Otherwise outStream.write()
      // will be completely skipped
      outStream.write(b, off, len);
    }
  }

  @Override
  public void flush() throws IOException {
    for (BufferedBlockOutStream outStream : mBlockOutStreams) {
      outStream.write(mBuffer.array(), 0, mBuffer.position());
      outStream.flush();
    }
    mBuffer.clear();
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    for (BufferedBlockOutStream outStream : mBlockOutStreams) {
      outStream.cancel();
    }
    mClosed = true;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    flush();
    for (BufferedBlockOutStream outStream : mBlockOutStreams) {
      outStream.close();
    }
    mClosed = true;
  }
}
