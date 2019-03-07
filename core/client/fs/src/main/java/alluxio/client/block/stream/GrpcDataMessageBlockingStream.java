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

package alluxio.client.block.stream;

import alluxio.exception.status.DeadlineExceededException;
import alluxio.grpc.DataMessage;
import alluxio.grpc.DataMessageMarshaller;
import alluxio.network.protocol.databuffer.DataBuffer;

import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A helper class for accessing gRPC bi-directional stream synchronously.
 *
 * @param <ReqT> type of the request
 * @param <ResT> type of the response
 */
@NotThreadSafe
public class GrpcDataMessageBlockingStream<ReqT, ResT> extends GrpcBlockingStream<ReqT, ResT> {
  private final DataMessageMarshaller<ResT> mMarshaller;

  /**
   * @param rpcFunc the gRPC bi-directional stream stub function
   * @param bufferSize maximum number of incoming messages the buffer can hold
   * @param description description of this stream
   * @param deserializer custom deserializer for the response
   */
  public GrpcDataMessageBlockingStream(Function<StreamObserver<ResT>, StreamObserver<ReqT>> rpcFunc,
      int bufferSize, String description, DataMessageMarshaller<ResT> deserializer) {
    super((resObserver) -> {
      DataMessageClientResponseObserver<ReqT, ResT> newObserver =
          new DataMessageClientResponseObserver<>(resObserver, deserializer);
      StreamObserver<ReqT> requestObserver = rpcFunc.apply(newObserver);
      return requestObserver;
    }, bufferSize, description);
    mMarshaller = deserializer;
  }

  @Override
  public ResT receive(long timeoutMs) throws IOException {
    DataMessage<ResT, DataBuffer> message = receiveDataMessage(timeoutMs);
    if (message == null) {
      return null;
    }
    return mMarshaller.combineData(message);
  }

  /**
   * Receives a response with data buffer from the server. Will wait until a response is received,
   * or throw an exception if times out. Caller of this method must release the buffer after reading
   * the data.
   *
   * @param timeoutMs maximum time to wait before giving up and throwing
   *                  a {@link DeadlineExceededException}
   * @return the response message with data buffer, or null if the inbound stream is completed
   * @throws IOException if any error occurs
   */
  public DataMessage<ResT, DataBuffer> receiveDataMessage(long timeoutMs) throws IOException {
    ResT response = super.receive(timeoutMs);
    if (response == null) {
      return null;
    }
    DataBuffer buffer = mMarshaller.pollBuffer(response);
    return new DataMessage<>(response, buffer);
  }

  @Override
  public void waitForComplete(long timeoutMs) throws IOException {
    DataMessage<ResT, DataBuffer> message;
    while (!isCanceled() && (message = receiveDataMessage(timeoutMs)) != null) {
      if (message.getBuffer() != null) {
        message.getBuffer().release();
      }
    }
    super.waitForComplete(timeoutMs);
  }
}
