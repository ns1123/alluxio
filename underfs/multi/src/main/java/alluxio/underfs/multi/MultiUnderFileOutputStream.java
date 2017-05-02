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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * HDFS implementation for {@link alluxio.underfs.UnderFileSystem}.
 */
@NotThreadSafe
public class MultiUnderFileOutputStream extends OutputStream {

  /** The underlying streams to read data from. */
  private Set<OutputStream> mStreams;

  /**
   * Creates a new instance of {@link MultiUnderFileOutputStream}.
   *
   * @param streams the underlying output streams
   */
  MultiUnderFileOutputStream(Set<OutputStream> streams) {
    mStreams = streams;
  }

  @Override
  public void write(int b) throws IOException {
    for (OutputStream stream : mStreams) {
      // TODO(jiri): error checking
      stream.write(b);
    }
  }
}
