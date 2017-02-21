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

import alluxio.proto.security.CapabilityProto;

import com.google.common.base.Throwables;
import com.google.protobuf.CodedInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Protobuf related utils.
 */
public final class ProtoUtils {
  private ProtoUtils() {} // prevent instantiation

  /**
   * A wrapper of {@link CodedInputStream#readRawVarint32(InputStream)}.
   *
   * @param firstByte first byte in the input stream
   * @param input input stream
   * @return an int value read from the input stream
   * @throws IOException if the proto message being parsed is invalid
   */
  public static int readRawVarint32(int firstByte, InputStream input) throws IOException {
    return CodedInputStream.readRawVarint32(firstByte, input);
  }
  // ALLUXIO CS ADD

  /**
   * A wrapper of
   * {@link alluxio.proto.journal.Job.TaskInfo.Builder#setResult} to take byte[] as input.
   *
   * @param builder the builder to update
   * @param bytes results bytes to set
   * @return updated builder
   */
  public static alluxio.proto.journal.Job.TaskInfo.Builder setResult(
      alluxio.proto.journal.Job.TaskInfo.Builder builder, byte[] bytes) {
    return builder.setResult(com.google.protobuf.ByteString.copyFrom(bytes));
  }

  /**
   * A wrapper of
   * {@link alluxio.proto.journal.Job.StartJobEntry.Builder#setSerializedJobConfig}
   * to take byte[] as input.
   *
   * @param builder the builder to update
   * @param bytes results bytes to set
   * @return updated builder
   */
  public static alluxio.proto.journal.Job.StartJobEntry.Builder setSerializedJobConfig(
      alluxio.proto.journal.Job.StartJobEntry.Builder builder, byte[] bytes) {
    return builder.setSerializedJobConfig(com.google.protobuf.ByteString.copyFrom(bytes));
  }

  /**
   * Encodes the given content as a byte array.
   *
   * @param content the content to encode
   * @return the encoded content
   */
  public static byte[] encode(alluxio.proto.security.CapabilityProto.Content content) {
    byte[] result = new byte[content.getSerializedSize()];
    com.google.protobuf.CodedOutputStream output =
        com.google.protobuf.CodedOutputStream.newInstance(result);
    try {
      content.writeTo(output);
    } catch (IOException e) {
      // This should never happen.
      throw Throwables.propagate(e);
    }
    output.checkNoSpaceLeft();
    return result;
  }

  /**
   * Decodes the given content.
   *
   * @param content the content to decode
   * @return the decoded content
   * @throws IOException if an error occurs
   */
  public static alluxio.proto.security.CapabilityProto.Content decode(byte[] content)
      throws IOException {
    try {
      return CapabilityProto.Content.parseFrom(content);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IOException(e);
    }
  }
  // ALLUXIO CS END
}
