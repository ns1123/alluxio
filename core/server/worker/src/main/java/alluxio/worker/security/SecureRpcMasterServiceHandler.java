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

package alluxio.worker.security;

import alluxio.RpcUtils;
import alluxio.grpc.SecureRpcMasterServiceGrpc;
import alluxio.grpc.WriteSecretKeyPRequest;
import alluxio.grpc.WriteSecretKeyPResponse;
import alluxio.proto.security.Key;
import alluxio.security.MasterKey;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * gRPC service handler for serving secure RPC requests.
 */
@ThreadSafe
public class SecureRpcMasterServiceHandler
    extends SecureRpcMasterServiceGrpc.SecureRpcMasterServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(SecureRpcMasterServiceHandler.class);
  /** BlockWorker master. */
  private WorkerProcess mWorkerProcess;

  /**
   * @param workerProcess worker process to bind incoming requests
   */
  public SecureRpcMasterServiceHandler(WorkerProcess workerProcess) {
    mWorkerProcess = workerProcess;
  }

  @Override
  public synchronized void writeSecretKey(WriteSecretKeyPRequest request,
      StreamObserver<WriteSecretKeyPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<WriteSecretKeyPResponse>) () -> {

      Key.SecretKey secretKey = request.getSecretKey();
      // Only Capability secret keys are supported for exchange.
      if (secretKey.getKeyType() != Key.KeyType.CAPABILITY) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Not supported SecretKey type: %s", secretKey.getKeyType()))
            .asRuntimeException();
      }

      MasterKey key;
      try {
        key = new MasterKey(secretKey.getKeyId(), secretKey.getExpirationTimeMs(),
            secretKey.getSecretKey().toByteArray());
      } catch (NoSuchAlgorithmException | InvalidKeyException e) {
        throw Status.INTERNAL
            .withDescription(String.format("Failed to convert given secret key:%s", e.toString()))
            .asRuntimeException();
      }

      mWorkerProcess.getWorker(BlockWorker.class).getCapabilityCache().setCapabilityKey(key);
      LOG.debug("Received secret key, id {}", key.getKeyId());

      return WriteSecretKeyPResponse.getDefaultInstance();
    }, "WriteSecretKey", "request=%s", responseObserver, request);
  }
}
