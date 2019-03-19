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

import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.util.network.SSLUtils;
import alluxio.worker.WorkerProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Runs a TLS enabled gRPC server for secret exchange.
 */
@NotThreadSafe
public final class SecureRpcServer {
  private static final Logger LOG = LoggerFactory.getLogger(SecureRpcServer.class);

  /** Underlying gRPC server.  */
  private GrpcServer mServer;

  /**
   * Creates a new instance of {@link SecureRpcServer}.
   *
   * @param hostName the host name of the server
   * @param bindAddress the bind server address
   * @param workerProcess the Alluxio worker process
   */
  public SecureRpcServer(final String hostName, final SocketAddress bindAddress,
      final WorkerProcess workerProcess) {
    try {
      mServer = createServerBuilder(hostName, bindAddress)
          .addService(new GrpcService(new SecureRpcMasterServiceHandler(workerProcess))).build()
          .start();
    } catch (IOException e) {
      LOG.error("SecureRPC Server failed to start on {}", bindAddress.toString(), e);
      throw new RuntimeException(e);
    }
    LOG.info("SecureRPC Server started, listening on {}", bindAddress.toString());
  }

  private GrpcServerBuilder createServerBuilder(String hostName, SocketAddress bindAddress) {
    return GrpcServerBuilder.forAddress(hostName, bindAddress, ServerConfiguration.global())
        .sslContext(SSLUtils.getSelfSignedServerSslContext());
  }

  /**
   * Shuts down the server.
   */
  public void shutdown() {
    if (mServer != null) {
      if (!mServer.shutdown()) {
        LOG.warn("RPC Server shutdown timed out.");
      }
    }
  }

  /**
   * @return the port the server has been bound to
   */
  public int getPort() {
    if (mServer == null) {
      return -1;
    }
    return mServer.getBindPort();
  }
}
