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

package alluxio.underfs.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Output stream implementation for {@link HdfsUnderFileSystem}. This class is just a wrapper on top
 * of an underlying {@link FSDataOutputStream}, except all calls to {@link #flush()} will be
 * converted to {@link FSDataOutputStream#sync()}. This is currently safe because all invocations of
 * flush intend the functionality to be sync.
 */
@NotThreadSafe
public class HdfsUnderFileOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUnderFileOutputStream.class);
  /** Underlying output stream. */
  final FSDataOutputStream mOut;

  /**
   * Basic constructor.
   *
   * @param out underlying stream to wrap
   */
  public HdfsUnderFileOutputStream(FSDataOutputStream out) {
    LOG.error("HdfsUnderFileOutputStream#constr classLoader {}", this.getClass().getClassLoader());
    LOG.error("HdfsUnderFileOutputStream#constr out classLoader {}",
        out.getClass().getClassLoader());
    LOG.error("HdfsUnderFileOutputStream#constr FSDataOutputStream {}",
        FSDataOutputStream.class.getClassLoader());
    try {
      LOG.error("HdfsUnderFileOutputStream url = {}",
          this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
      LOG.error("FSDataOutputStream url = {}",
          FSDataOutputStream.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (Exception e) {
      // ignore
    }
    mOut = out;
  }

  @Override
  public void close() throws IOException {
    mOut.close();
  }

  @Override
  public void flush() throws IOException {
    // TODO(calvin): This functionality should be restricted to select output streams.
    mOut.sync();
  }

  @Override
  public void write(int b) throws IOException {
    mOut.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mOut.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    LOG.error("HdfsUnderFileOutputStream#write mOut classLoader {}",
        mOut.getClass().getClassLoader());
    mOut.write(b, off, len);
  }
}
