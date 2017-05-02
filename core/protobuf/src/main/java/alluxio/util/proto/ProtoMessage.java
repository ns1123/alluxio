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

package alluxio.util.proto;

import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * A simple wrapper around the MessageLite class in Protobuf for a few messages defined and
 * generated in Alluxio. In other parts of Alluxio code base that are outside of this module,
 * use this class to replace MessageLite when it must reference MessageLite as a base class of
 * different generated messages. This class is intended to be used internally only.
 */
public final class ProtoMessage {
  private MessageLite mMessage;

  /**
   * Constructs a {@link ProtoMessage} instance wrapping around {@link MessageLite}.
   *
   * @param message the message to wrap
   */
<<<<<<< HEAD
  public enum Type {
    READ_REQUEST,
    WRITE_REQUEST,
    RESPONSE,
    // ALLUXIO CS ADD
    SASL_MESSAGE,
    SECRET_KEY,
    // ALLUXIO CS END
=======
  public ProtoMessage(MessageLite message) {
    mMessage = message;
>>>>>>> os/master
  }

  /**
   * Gets the read request or throws runtime exception if mMessage is not of type
   * {@link Protocol.ReadRequest}.
   *
   * @return the read request
   */
  public Protocol.ReadRequest asReadRequest() {
    Preconditions.checkState(mMessage instanceof Protocol.ReadRequest);
    return (Protocol.ReadRequest) mMessage;
  }

  /**
   * @return true if mMessage is of type {@link Protocol.ReadRequest}
   */
  public boolean isReadRequest() {
    return mMessage instanceof Protocol.ReadRequest;
  }

  /**
   * Gets the write request or throws runtime exception if mMessage is not of type
   * {@link Protocol.WriteRequest}.
   *
   * @return the write request
   */
  public Protocol.WriteRequest asWriteRequest() {
    Preconditions.checkState(mMessage instanceof Protocol.WriteRequest);
    return (Protocol.WriteRequest) mMessage;
  }

  // ALLUXIO CS ADD
  /**
   * Constructs a {@link ProtoMessage} instance wrapping around
   * {@link alluxio.proto.security.Key.SecretKey}.
   *
   * @param message the message to wrap
   */
  public ProtoMessage(alluxio.proto.security.Key.SecretKey message) {
    this(message, Type.SECRET_KEY);
  }

  /**
   * Constructs a {@link ProtoMessage} instance wrapping around {@link Protocol.SaslMessage}.
   *
   * @param message the message to wrap
   */
  public ProtoMessage(Protocol.SaslMessage message) {
    this(message, Type.SASL_MESSAGE);
  }
  // ALLUXIO CS END
  /**
   * @return true if mMessage is of type {@link Protocol.WriteRequest}
   */
  public boolean isWriteRequest() {
    return mMessage instanceof Protocol.WriteRequest;
  }

  /**
   * Gets the response or throws runtime exception if mMessage is not of type
   * {@link Protocol.Response}.
   *
   * @return the response
   */
  public Protocol.Response asResponse() {
    Preconditions.checkState(mMessage instanceof Protocol.Response);
    return (Protocol.Response) mMessage;
  }

  /**
   * @return true if mMessage is of type {@link Protocol.Response}
   */
  public boolean isResponse() {
    return mMessage instanceof Protocol.Response;
  }

  /**
   * @return the serialized message as byte array
   */
  public byte[] toByteArray() {
    return mMessage.toByteArray();
  }

  /**
   * Parses proto message from bytes given a prototype.
   *
   * @param serialized the serialized message
   * @param prototype the prototype of the message to return which is usually constructed via
   *        new ProtoMessage(SomeProtoType.getDefaultInstance())
   * @return the proto message
   */
  public static ProtoMessage parseFrom(byte[] serialized, ProtoMessage prototype) {
    try {
<<<<<<< HEAD
      switch (type) {
        case READ_REQUEST:
          message = Protocol.ReadRequest.parseFrom(serialized);
          break;
        case WRITE_REQUEST:
          message = Protocol.WriteRequest.parseFrom(serialized);
          break;
        case RESPONSE:
          message = Protocol.Response.parseFrom(serialized);
          break;
        // ALLUXIO CS ADD
        case SASL_MESSAGE:
          message = Protocol.SaslMessage.parseFrom(serialized);
          break;
        case SECRET_KEY:
          message = alluxio.proto.security.Key.SecretKey.parseFrom(serialized);
          break;
        // ALLUXIO CS END
        default:
          throw new IllegalArgumentException("Unknown class type " + type.toString());
      }
      return new ProtoMessage(message, type);
=======
      return new ProtoMessage(prototype.mMessage.getParserForType().parseFrom(serialized));
>>>>>>> os/master
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
