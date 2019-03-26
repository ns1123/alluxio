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

package alluxio.security.capability;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.SecureRpcMasterServiceGrpc;
import alluxio.grpc.WriteSecretKeyPRequest;
import alluxio.proto.security.Key;
import alluxio.security.MasterKey;
import alluxio.util.network.SSLUtils;
import alluxio.util.proto.ProtoUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.net.InetSocketAddress;

/**
 * Client for writing secret keys.
 */
@ThreadSafe
public class SecretKeyWriter {

  /**
   * Writes a new capability key to a given address.
   *
   * @param address target host address
   * @param capabilityKey the capability key
   * @param conf Alluxio configuration
   * @throws AlluxioStatusException
   */
  public static synchronized void writeCapabilityKey(InetSocketAddress address,
      MasterKey capabilityKey, AlluxioConfiguration conf) throws AlluxioStatusException {
    GrpcChannel secureChannel = buildSecureChannel(address, conf);
    try {
      // Create a gRPC stub for contacting the host via secured channel.
      SecureRpcMasterServiceGrpc.SecureRpcMasterServiceBlockingStub client =
          SecureRpcMasterServiceGrpc.newBlockingStub(secureChannel);
      // Convert given capability key to a proto SecretKey.
      Key.SecretKey secretKey =
          ProtoUtils
              .setSecretKey(
                  Key.SecretKey.newBuilder().setKeyType(Key.KeyType.CAPABILITY)
                      .setKeyId(capabilityKey.getKeyId())
                      .setExpirationTimeMs(capabilityKey.getExpirationTimeMs()),
                  capabilityKey.getEncodedKey())
              .build();
      // Write capability key to target host.
      client.writeSecretKey(WriteSecretKeyPRequest.newBuilder().setSecretKey(secretKey).build());
    } catch (Exception e) {
      throw AlluxioStatusException.fromCheckedException(e);
    } finally {
      secureChannel.shutdown();
    }
  }

  private static GrpcChannel buildSecureChannel(InetSocketAddress address,
      AlluxioConfiguration conf) throws AlluxioStatusException {
    // Create a gRPC channel with Ssl context.
    return GrpcChannelBuilder.newBuilder(new GrpcServerAddress(address), conf)
        .sslContext(SSLUtils.getSelfSignedClientSslContext()).build();
  }
}
