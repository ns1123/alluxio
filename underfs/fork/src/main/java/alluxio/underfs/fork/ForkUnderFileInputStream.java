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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Virtual input stream that can be used to read from multiple UFSes.
 *
 * The implementation maintains the offset of the aggregate input stream and an input stream for
 * each of the underlying UFSes as well as their offsets. It also maintains the offset of the
 * aggregate input stream. A read operation reads bytes from one of the streams, using the aggregate
 * and underlying offset information to make sure that the underlying stream is read from at the
 * correct position.
 */
@NotThreadSafe
public class ForkUnderFileInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(ForkUnderFileInputStream.class);

  /**
   * The underlying streams to read data from and their current offset.
   */
  private List<Pair<InputStream, AtomicReference<Long>>> mStreams = new ArrayList<>();
  /**
   * The current aggregate stream offset. The invariant maintain by the implementation is that
   * this offset is greater or equal to any offset of an underlying stream.
   */
  private long mOffset;

  /**
   * Creates a new instance of {@link ForkUnderFileInputStream}.
   *
   * @param streams the underlying input streams
   */
  ForkUnderFileInputStream(List<InputStream> streams) {
    for (InputStream stream : streams) {
      mStreams.add(new ImmutablePair<>(stream, new AtomicReference<>(0L)));
    }
    mOffset = 0;
  }

  @Override
  public void close() throws IOException {
    ForkUnderFileSystemUtils
        .invokeAll(new Function<Pair<InputStream, AtomicReference<Long>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<InputStream, AtomicReference<Long>> arg) {
            InputStream is = arg.getLeft();
            try {
              is.close();
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mStreams);
  }

  @Override
  public int read() throws IOException {
    AtomicReference<Integer> result = new AtomicReference<>();
    // The function takes Pair<Pair<A,B>,C> as input
    // - A is the input stream to attempt the read operation with
    // - B is a reference to the offset of the input stream
    // - C is a reference to the return value of the read operation
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<InputStream, AtomicReference<Long>>, AtomicReference<Integer>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<InputStream, AtomicReference<Long>>, AtomicReference<Integer>> arg) {
            InputStream stream = arg.getLeft().getLeft();
            AtomicReference<Long> offset = arg.getLeft().getRight();
            AtomicReference<Integer> result = arg.getRight();
            Preconditions.checkState(offset.get() <= mOffset);
            // 1. make sure the underlying stream is at the right offset
            if (offset.get() < mOffset) {
              try {
                long delta = offset.get() - mOffset;
                if (stream.skip(delta) != delta) {
                  throw new IOException("stream could not be advanced");
                }
                offset.set(offset.get() + delta);
              } catch (IOException e) {
                return e;
              }
            }
            // 2. attempt the read operation
            try {
              int n = stream.read();
              result.set(n);
              if (n != -1) {
                mOffset += 1;
              }
              offset.set(offset.get() + 1);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mStreams, result);
    return result.get();
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    AtomicReference<Integer> result = new AtomicReference<>();
    // The function takes Pair<Pair<A,B>,C> as input
    // - A is the input stream to attempt the read operation with
    // - B is a reference to the offset of the input stream
    // - C is a reference to the return value of the read operation
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<InputStream, AtomicReference<Long>>, AtomicReference<Integer>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<InputStream, AtomicReference<Long>>, AtomicReference<Integer>> arg) {
            InputStream stream = arg.getLeft().getLeft();
            AtomicReference<Long> offset = arg.getLeft().getRight();
            AtomicReference<Integer> result = arg.getRight();
            Preconditions.checkState(offset.get() <= mOffset);
            // 1. make sure the underlying stream is at the right offset
            if (offset.get() < mOffset) {
              try {
                long delta = offset.get() - mOffset;
                if (stream.skip(delta) != delta) {
                  throw new IOException("stream could not be advanced");
                }
                offset.set(offset.get() + delta);
              } catch (IOException e) {
                return e;
              }
            }
            // 2. attempt the read operation
            try {
              int n = stream.read(b, off, len);
              result.set(n);
              if (n != -1) {
                mOffset += n;
              }
              offset.set(offset.get() + n);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mStreams, result);
    return result.get();
  }
}
