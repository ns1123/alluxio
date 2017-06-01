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

package alluxio.underfs.fork;

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Virtual input stream that can be used to read from multiple UFSes.
 */
@NotThreadSafe
public class ForkUnderFileInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(ForkUnderFileInputStream.class);

  /** The current stream offset. */
  private long mOffset;
  /** The options for opening an underlying stream. */
  private final OpenOptions mOptions;
  /** An input stream for an underlying UFS. */
  private InputStream mStream;
  /** Pairs of underlying UFS paths and instances to read from. */
  private final Collection<Pair<String, UnderFileSystem>> mUfses;

  /**
   * Creates a new instance of {@link ForkUnderFileInputStream}.
   *
   * @param ufses pairs of UFS paths and instances
   * @param options the options for opening an underlying stream
   */
  ForkUnderFileInputStream(Collection<Pair<String, UnderFileSystem>> ufses, OpenOptions options) {
    mOffset = 0;
    mOptions = options;
    mStream = null;
    mUfses = ufses;
  }

  @Override
  public void close() throws IOException {
    if (mStream != null) {
      mStream.close();
    }
    mStream = null;
  }

  @Override
  public int read() throws IOException {
    // If a valid stream exists, read from it.
    if (mStream != null) {
      try {
        int n = mStream.read();
        if (n != -1) {
          mOffset++;
        }
        return n;
      } catch (IOException e) {
        cleanup();
      }
    }

    // Otherwise, try to read from the UFSes one by one.
    AtomicReference<Integer> result = new AtomicReference<>();
    // The function takes Pair<Pair<A,B>,C> as input:
    //
    // - A is the UFS to attempt the read operation with
    // - B is the UFS path to read from
    // - C is a reference to the return value of the read operation
    //
    // Pre-condition: mStream == null
    // Post-condition: mStream != null iff the function returned null
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Integer>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Integer>> arg) {
            Preconditions.checkState(mStream == null);
            String path = arg.getLeft().getLeft();
            UnderFileSystem ufs = arg.getLeft().getRight();
            AtomicReference<Integer> result = arg.getRight();
            try {
              mStream = ufs.open(path, mOptions);
              long numSkipped = mStream.skip(mOffset);
              if (numSkipped != mOffset) {
                mStream.close();
                throw new IOException("Failed to skip to the correct offset");
              }
              int n = mStream.read();
              if (n != -1) {
                mOffset++;
              }
              result.set(n);
              return null;
            } catch (IOException e) {
              cleanup();
              return e;
            }
          }
        }, ForkUnderFileSystemUtils.fold(mUfses, result));
    return result.get();
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    // If a valid stream exists, read from it.
    if (mStream != null) {
      try {
        int n = mStream.read(b, off, len);
        if (n != -1) {
          mOffset += n;
        }
        return n;
      } catch (IOException e) {
        cleanup();
      }
    }
    // Otherwise try the UFSes one by one.
    AtomicReference<Integer> result = new AtomicReference<>();
    // The function takes Pair<Pair<A,B>,C> as input
    //
    // - A is the UFS to attempt the read operation with
    // - B is the UFS path to read from
    // - C is a reference to the return value of the read operation
    //
    // Pre-condition: mStream == null
    // Post-condition: mStream != null iff the function returned null
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Integer>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Integer>> arg) {
            Preconditions.checkState(mStream == null);
            String path = arg.getLeft().getLeft();
            UnderFileSystem ufs = arg.getLeft().getRight();
            AtomicReference<Integer> result = arg.getRight();
            try {
              mStream = ufs.open(path, mOptions);
              long numSkipped = mStream.skip(mOffset);
              if (numSkipped != mOffset) {
                throw new IOException("Failed to skip to the correct offset");
              }
              int n = mStream.read(b, off, len);
              if (n != -1) {
                mOffset += n;
              }
              result.set(n);
              return null;
            } catch (IOException e) {
              cleanup();
              return e;
            }
          }
        }, ForkUnderFileSystemUtils.fold(mUfses, result));
    return result.get();
  }

  @Override
  public long skip(long n) throws IOException {
    if (mStream != null) {
      try {
        long skipped = mStream.skip(n);
        mOffset += skipped;
        return skipped;
      } catch (IOException e) {
        cleanup();
      }
    }
    mOffset += n;
    return n;
  }

  private void cleanup() {
    if (mStream == null) {
      return;
    }
    try {
      mStream.close();
    } catch (IOException e) {
      LOG.warn("Failed to close stream {}", e.getMessage());
      LOG.debug("Exception: ", e);
    }
    mStream = null;
  }
}
