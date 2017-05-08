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
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Virtual input stream that can be used to read from multiple UFSes.
 */
@NotThreadSafe
public class ForkUnderFileInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(ForkUnderFileInputStream.class);

  /** The underlying streams to read data from. */
  private List<InputStream> mStreams;

  /**
   * Creates a new instance of {@link ForkUnderFileInputStream}.
   *
   * @param streams the underlying input streams
   */
  ForkUnderFileInputStream(List<InputStream> streams) {
    mStreams = streams;
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
    }, mStreams);
  }

  @Override
  public int read() throws IOException {
    AtomicReference<Integer> result = new AtomicReference<>();
    ForkUnderFileSystemUtils
        .invokeOne(new Function<Pair<InputStream, AtomicReference<Integer>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<InputStream, AtomicReference<Integer>> arg) {
            try {
              arg.getValue().set(arg.getKey().read());
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mStreams, result);
    return result.get();
  }
}
