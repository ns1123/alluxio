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

package alluxio.underfs.multi;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
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
 * HDFS implementation for {@link alluxio.underfs.UnderFileSystem}.
 */
@NotThreadSafe
public class MultiUnderFileInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(MultiUnderFileInputStream.class);

  /** The underlying streams to read data from. */
  private List<InputStream> mStreams;

  /**
   * Creates a new instance of {@link MultiUnderFileInputStream}.
   *
   * @param streams the underlying input streams
   */
  MultiUnderFileInputStream(List<InputStream> streams) {
    mStreams = streams;
  }

  @Override
  public void close() throws IOException {
    MultiUnderFileSystemUtils.invokeAll(new Function<InputStream, IOException>() {
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
    MultiUnderFileSystemUtils
        .invokeOne(new Function<InputOutput<InputStream, AtomicReference<Integer>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(InputOutput<InputStream, AtomicReference<Integer>> arg) {
            try {
              arg.getOutput().set(arg.getInput().read());
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mStreams, result);
    return result.get();
  }
}
