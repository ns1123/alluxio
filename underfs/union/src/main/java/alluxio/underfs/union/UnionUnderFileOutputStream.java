/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Virtual output stream that can be used to synchronously write to multiple output streams.
 */
@NotThreadSafe
public class UnionUnderFileOutputStream extends OutputStream {

  /** The underlying streams to read data from. */
  private final Collection<OutputStream> mStreams;
  /** The executor service to use. */
  private final ExecutorService mExecutorService;

  /**
   * Creates a new instance of {@link UnionUnderFileOutputStream}.
   *
   * @param service the executor service to use
   * @param streams the underlying output streams
   */
  UnionUnderFileOutputStream(ExecutorService service, Collection<OutputStream> streams) {
    mExecutorService = service;
    mStreams = streams;
  }

  @Override
  public void close() throws IOException {
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, OutputStream::close, mStreams);
  }

  @Override
  public void flush() throws IOException {
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, OutputStream::flush, mStreams);
  }

  @Override
  public void write(final int b) throws IOException {
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, os -> os.write(b), mStreams);
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, os -> os.write(b, off, len), mStreams);
  }
}
