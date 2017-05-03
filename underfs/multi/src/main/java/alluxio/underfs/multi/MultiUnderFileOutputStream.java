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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * HDFS implementation for {@link alluxio.underfs.UnderFileSystem}.
 */
@NotThreadSafe
public class MultiUnderFileOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(MultiUnderFileOutputStream.class);

  /** The underlying streams to read data from. */
  private List<OutputStream> mStreams;

  /**
   * Creates a new instance of {@link MultiUnderFileOutputStream}.
   *
   * @param streams the underlying output streams
   */
  MultiUnderFileOutputStream(List<OutputStream> streams) {
    mStreams = streams;
  }

  @Override
  public void close() throws IOException {
    MultiUnderFileSystemUtils.invokeAll(new Function<OutputStream, IOException>() {
      @Nullable
      @Override
      public IOException apply(OutputStream os) {
        try {
          os.close();
        } catch (IOException e) {
          return e;
        }
        return null;
      }
    }, mStreams);
  }

  @Override
  public void flush() throws IOException {
    MultiUnderFileSystemUtils.invokeAll(new Function<OutputStream, IOException>() {
      @Nullable
      @Override
      public IOException apply(OutputStream os) {
        try {
          os.flush();
        } catch (IOException e) {
          return e;
        }
        return null;
      }
    }, mStreams);
  }

  @Override
  public void write(final int b) throws IOException {
    MultiUnderFileSystemUtils.invokeAll(new Function<OutputStream, IOException>() {
      @Nullable
      @Override
      public IOException apply(OutputStream os) {
        try {
          os.write(b);
        } catch (IOException e) {
          return e;
        }
        return null;
      }
    }, mStreams);
  }
}
