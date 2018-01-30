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

package alluxio.network.netty;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.security.Key;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.LoginUser;
import alluxio.security.capability.CapabilityKey;
import alluxio.util.network.NettyUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.util.proto.ProtoUtils;

import com.codahale.metrics.Counter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Write a secret key to a remote server using Netty secured by SSL.
 */
public final class NettySecretKeyWriter {
  private static final Logger LOG = LoggerFactory.getLogger(NettySecretKeyWriter.class);

  private NettySecretKeyWriter() {}  // prevent instantiation

  /**
   * Writes a {@link CapabilityKey} to the Netty server and waits for the response.
   *
   * @param address the network address of the secret key server
   * @param capabilityKey the capability key to send
   * @throws IOException if it fails to write capability key to the remote server
   */
  public static void write(InetSocketAddress address, CapabilityKey capabilityKey)
      throws IOException {
    Channel channel = null;
    Metrics.NETTY_SECRET_KEY_WRITE_OPS.inc();
    RetryPolicy retryPolicy = new CountingRetry(1);
    try {
      Bootstrap bs = NettySecureRpcClient.createClientBootstrap(address);
      bs.attr(alluxio.netty.NettyAttributes.HOSTNAME_KEY, address.getHostName());
      boolean authenticated = false;
      do {
        try {
          channel = bs.connect().sync().channel();
          NettyUtils.waitForClientChannelReady(channel);
          authenticated = true;
        } catch (Exception e) {
          LOG.info("Failed to build an authenticated channel. "
              + "This may be due to Kerberos credential expiration. Retry login.");
          channel.close();
          LoginUser.relogin();
        }
      } while (retryPolicy.attemptRetry() && !authenticated);
      if (!authenticated) {
        throw new IOException("Failed to build an authenticated channel even after relogin");
      }
      Key.SecretKey request =
          ProtoUtils.setSecretKey(
              Key.SecretKey.newBuilder().setKeyType(Key.KeyType.CAPABILITY)
                  .setKeyId(capabilityKey.getKeyId())
                  .setExpirationTimeMs(capabilityKey.getExpirationTimeMs()),
              capabilityKey.getEncodedKey()).build();
      NettyRPC.call(NettyRPCContext.defaults().setChannel(channel).setTimeout(
          Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS)),
          new ProtoMessage(request));
    } catch (Exception e) {
      Metrics.NETTY_SECRET_KEY_WRITE_FAILURES.inc();
      throw new IOException(e);
    } finally {
      if (channel != null) {
        channel.close();
      }
    }
  }

  /**
   * Class that contains metrics about {@link NettySecretKeyWriter}.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter NETTY_SECRET_KEY_WRITE_OPS =
        MetricsSystem.clientCounter("NettySecretKeyWriteOps");
    private static final Counter NETTY_SECRET_KEY_WRITE_FAILURES =
        MetricsSystem.clientCounter("NettySecretKeyWriteFailures");

    private Metrics() {} // prevent instantiation
  }
}
