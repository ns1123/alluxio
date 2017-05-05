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

package alluxio.client.netty;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.security.Key;
import alluxio.proto.status.Status.PStatus;
import alluxio.security.capability.CapabilityKey;
import alluxio.util.proto.ProtoMessage;
import alluxio.util.proto.ProtoUtils;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Write a secret key to a remote server using Netty secured by SSL.
 */
@NotThreadSafe
public class NettySecretKeyWriter {
  private static final Logger LOG = LoggerFactory.getLogger(NettySecretKeyWriter.class);

  private boolean mOpen;
  private InetSocketAddress mAddress;

  /**
   * Creates a new {@link NettySecretKeyWriter}.
   */
  public NettySecretKeyWriter() {
    mOpen = false;
    mAddress = null;
  }

  /**
   * Opens the writer.
   *
   * @param address the remote server address
   */
  public void open(InetSocketAddress address) {
    Preconditions.checkState(!mOpen);
    mAddress = address;
    mOpen = true;
  }

  /**
   * Closes the writer.
   */
  public void close() {
    if (mOpen) {
      mOpen = false;
    }
  }

  /**
   * Writes a {@link CapabilityKey} to the Netty server and waits for the response.
   *
   * @param capabilityKey the capability key to send
   * @throws IOException if it fails to write capability key to the remote server
   */
  public void write(CapabilityKey capabilityKey) throws IOException {
    Channel channel = null;
    ClientHandler handler = null;
    Metrics.NETTY_SECRET_KEY_WRITE_OPS.inc();
    try {
      Bootstrap bs = NettySecureRpcClient.createClientBootstrap(mAddress);
      channel = bs.connect().sync().channel();
      NettySecureRpcClient.waitForChannelReady(channel);
      if (!(channel.pipeline().last() instanceof ClientHandler)) {
        channel.pipeline().addLast(new ClientHandler());
      }
      handler = (ClientHandler) channel.pipeline().last();
      SingleResponseListener listener = new SingleResponseListener();
      handler.addListener(listener);

      Key.SecretKey request =
          ProtoUtils.setSecretKey(
              Key.SecretKey.newBuilder().setKeyType(Key.KeyType.CAPABILITY)
                  .setKeyId(capabilityKey.getKeyId())
                  .setExpirationTimeMs(capabilityKey.getExpirationTimeMs()),
              capabilityKey.getEncodedKey()).build();

      ChannelFuture channelFuture =
          channel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(request), null)).sync();

      if (channelFuture.isDone() && !channelFuture.isSuccess()) {
        LOG.error("Failed to write secret key to %s for with error %s.", mAddress.toString(),
            channelFuture.cause());
        throw new IOException(channelFuture.cause());
      }

      RPCProtoMessage resp = listener
          .get(Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS),
              TimeUnit.MILLISECONDS);
      Protocol.Response response = resp.getMessage().asResponse();

      if (!response.getStatus().equals(PStatus.OK)) {
        throw new IOException("Failed to write capability key to the remote server.");
      }
    } catch (Exception e) {
      Metrics.NETTY_SECRET_KEY_WRITE_FAILURES.inc();
      throw new IOException(e);
    } finally {
      if (handler != null) {
        handler.removeListeners();
      }
      try {
        if (channel != null) {
          channel.close().sync();
        }
      } catch (InterruptedException ee) {
        Throwables.propagate(ee);
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
