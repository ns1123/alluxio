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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;

/**
 * TProtocol which delegates all operations to a supplied delegate.
 */
public class DelegatingTProtocol extends TProtocol {

  private final TProtocol mProtocol;

  /**
   * @param delegate the protocol to delegate to
   * @param transport the transport to use
   */
  public DelegatingTProtocol(TProtocol delegate, TTransport transport) {
    super(transport);
    mProtocol = delegate;
  }

  @Override
  public void writeMessageBegin(TMessage message) throws TException {
    mProtocol.writeMessageBegin(message);
  }

  @Override
  public void writeMessageEnd() throws TException {
    mProtocol.writeMessageEnd();
  }

  @Override
  public void writeStructBegin(TStruct struct) throws TException {
    mProtocol.writeStructBegin(struct);
  }

  @Override
  public void writeStructEnd() throws TException {
    mProtocol.writeStructEnd();
  }

  @Override
  public void writeFieldBegin(TField field) throws TException {
    mProtocol.writeFieldBegin(field);
  }

  @Override
  public void writeFieldEnd() throws TException {
    mProtocol.writeFieldEnd();
  }

  @Override
  public void writeFieldStop() throws TException {
    mProtocol.writeFieldStop();
  }

  @Override
  public void writeMapBegin(TMap map) throws TException {
    mProtocol.writeMapBegin(map);
  }

  @Override
  public void writeMapEnd() throws TException {
    mProtocol.writeMapEnd();
  }

  @Override
  public void writeListBegin(TList list) throws TException {
    mProtocol.writeListBegin(list);
  }

  @Override
  public void writeListEnd() throws TException {
    mProtocol.writeListEnd();
  }

  @Override
  public void writeSetBegin(TSet set) throws TException {
    mProtocol.writeSetBegin(set);
  }

  @Override
  public void writeSetEnd() throws TException {
    mProtocol.writeSetEnd();
  }

  @Override
  public void writeBool(boolean b) throws TException {
    mProtocol.writeBool(b);
  }

  @Override
  public void writeByte(byte b) throws TException {
    mProtocol.writeByte(b);
  }

  @Override
  public void writeI16(short i16) throws TException {
    mProtocol.writeI16(i16);
  }

  @Override
  public void writeI32(int i32) throws TException {
    mProtocol.writeI32(i32);
  }

  @Override
  public void writeI64(long i64) throws TException {
    mProtocol.writeI64(i64);
  }

  @Override
  public void writeDouble(double dub) throws TException {
    mProtocol.writeDouble(dub);
  }

  @Override
  public void writeString(String str) throws TException {
    mProtocol.writeString(str);
  }

  @Override
  public void writeBinary(ByteBuffer buf) throws TException {
    mProtocol.writeBinary(buf);
  }

  @Override
  public TMessage readMessageBegin() throws TException {
    return mProtocol.readMessageBegin();
  }

  @Override
  public void readMessageEnd() throws TException {
    mProtocol.readMessageEnd();
  }

  @Override
  public TStruct readStructBegin() throws TException {
    return mProtocol.readStructBegin();
  }

  @Override
  public void readStructEnd() throws TException {
    mProtocol.readStructEnd();
  }

  @Override
  public TField readFieldBegin() throws TException {
    return mProtocol.readFieldBegin();
  }

  @Override
  public void readFieldEnd() throws TException {
    mProtocol.readFieldEnd();
  }

  @Override
  public TMap readMapBegin() throws TException {
    return mProtocol.readMapBegin();
  }

  @Override
  public void readMapEnd() throws TException {
    mProtocol.readMapEnd();
  }

  @Override
  public TList readListBegin() throws TException {
    return mProtocol.readListBegin();
  }

  @Override
  public void readListEnd() throws TException {
    mProtocol.readListEnd();
  }

  @Override
  public TSet readSetBegin() throws TException {
    return mProtocol.readSetBegin();
  }

  @Override
  public void readSetEnd() throws TException {
    mProtocol.readSetEnd();
  }

  @Override
  public boolean readBool() throws TException {
    return mProtocol.readBool();
  }

  @Override
  public byte readByte() throws TException {
    return mProtocol.readByte();
  }

  @Override
  public short readI16() throws TException {
    return mProtocol.readI16();
  }

  @Override
  public int readI32() throws TException {
    return mProtocol.readI32();
  }

  @Override
  public long readI64() throws TException {
    return mProtocol.readI64();
  }

  @Override
  public double readDouble() throws TException {
    return mProtocol.readDouble();
  }

  @Override
  public String readString() throws TException {
    return mProtocol.readString();
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    return mProtocol.readBinary();
  }
}
