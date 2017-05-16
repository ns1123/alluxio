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
 * The implementation maintains an input stream for each of the underlying UFSes. A read
 * operation reads bytes from one of the streams and uses {@code skip} to advance all other
 * streams. If reading from a stream results in an exception or a stream cannot be advanced to
 * match the number of bytes read from another stream, the stream becomes invalid and it is not
 * used for future read operations.
 */
@NotThreadSafe
public class ForkUnderFileInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(ForkUnderFileInputStream.class);

  /**
   * The underlying streams to read data from.
   */
  private List<InputStream> mAllStreams;
  private List<InputStream> mValidStreams;
  private long mReadBytes;

  /**
   * Creates a new instance of {@link ForkUnderFileInputStream}.
   *
   * @param streams the underlying input streams
   */
  ForkUnderFileInputStream(List<InputStream> streams) {
    mAllStreams = streams;
    mValidStreams = streams;
    mReadBytes = 0;
  }

  @Override
  public void close() throws IOException {
    ForkUnderFileSystemUtils.invokeAll(new Function<InputStream, IOException>() {
      @Nullable
      @Override
      public IOException apply(InputStream is) {
        try {
          is.close();
        } catch (IOException e) {
          return e;
        }
        return null;
      }
    }, mAllStreams);
  }

  @Override
  public int read() throws IOException {
    Pair<? extends List<InputStream>, AtomicReference<Integer>> result =
        new MutablePair<>(new ArrayList<InputStream>(), new AtomicReference<Integer>());
    ForkUnderFileSystemUtils.invokeSome(
        new Function<Pair<InputStream, Pair<? extends List<InputStream>,
            AtomicReference<Integer>>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<InputStream, Pair<? extends List<InputStream>, AtomicReference<Integer>>> arg) {
            InputStream stream = arg.getLeft();
            List<InputStream> validStreams = arg.getRight().getLeft();
            AtomicReference<Integer> result = arg.getRight().getRight();
            try {
              if (result.get() == null) {
                result.set(stream.read());
                validStreams.add(stream);
              } else {
                int n = result.get();
                if (n == -1 || stream.skip(1) == 1) {
                  validStreams.add(stream);
                }
              }
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mValidStreams, result);
    mValidStreams = result.getLeft();
    return result.getRight().get();
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    Pair<? extends List<InputStream>, AtomicReference<Integer>> result =
        new MutablePair<>(new ArrayList<InputStream>(), new AtomicReference<Integer>());
    ForkUnderFileSystemUtils.invokeSome(
        new Function<Pair<InputStream, Pair<? extends List<InputStream>,
            AtomicReference<Integer>>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<InputStream, Pair<? extends List<InputStream>, AtomicReference<Integer>>> arg) {
            InputStream stream = arg.getLeft();
            List<InputStream> validStreams = arg.getRight().getLeft();
            AtomicReference<Integer> result = arg.getRight().getRight();
            try {
              int n;
              if (result.get() == null) {
                n = stream.read(b, off, len);
                result.set(n);
                validStreams.add(stream);
              } else {
                n = result.get();
                if (n == -1 || stream.skip(n) == n) {
                  validStreams.add(stream);
                }
              }
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mValidStreams, result);
    mValidStreams = result.getLeft();
    return result.getRight().get();
  }
}
