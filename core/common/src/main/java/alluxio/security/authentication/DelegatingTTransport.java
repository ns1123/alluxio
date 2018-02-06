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

package alluxio.security.authentication;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Transport which delegates all methods to a given transport.
 */
public class DelegatingTTransport extends TTransport {
  private final TTransport mTransport;

  /**
   * @param transport the transport
   */
  public DelegatingTTransport(TTransport transport) {
    mTransport = transport;
  }

  @Override
  public boolean isOpen() {
    return mTransport.isOpen();
  }

  @Override
  public boolean peek() {
    return mTransport.peek();
  }

  @Override
  public void open() throws TTransportException {
    mTransport.open();
  }

  @Override
  public void close() {
    mTransport.close();
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    return mTransport.read(buf, off, len);
  }

  @Override
  public int readAll(byte[] buf, int off, int len) throws TTransportException {
    return mTransport.readAll(buf, off, len);
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    mTransport.write(buf, off, len);
  }

  @Override
  public void write(byte[] buf) throws TTransportException {
    mTransport.write(buf);
  }

  @Override
  public void flush() throws TTransportException {
    mTransport.flush();
  }

  @Override
  public byte[] getBuffer() {
    return mTransport.getBuffer();
  }

  @Override
  public int getBufferPosition() {
    return mTransport.getBufferPosition();
  }

  @Override
  public int getBytesRemainingInBuffer() {
    return mTransport.getBytesRemainingInBuffer();
  }

  @Override
  public void consumeBuffer(int len) {
    mTransport.consumeBuffer(len);
  }
}
