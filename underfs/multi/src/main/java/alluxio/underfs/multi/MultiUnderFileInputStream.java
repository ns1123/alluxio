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

import alluxio.Seekable;
import alluxio.exception.ExceptionMessage;

import com.google.common.collect.Iterables;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * HDFS implementation for {@link alluxio.underfs.UnderFileSystem}.
 */
@NotThreadSafe
public class MultiUnderFileInputStream extends InputStream {

  /** The underlying streams to read data from. */
  private Set<InputStream> mStreams;

  /**
   * Creates a new instance of {@link MultiUnderFileInputStream}.
   *
   * @param streams the underlying input streams
   */
  MultiUnderFileInputStream(Set<InputStream> streams) {
    mStreams = streams;
  }

  @Override
  public int read() throws IOException {
    return Iterables.getFirst(mStreams, null).read();
  }
}
