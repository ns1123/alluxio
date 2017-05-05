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
import alluxio.network.ChannelType;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;
import alluxio.security.authentication.AuthType;
import alluxio.util.network.NettyUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.ssl.SSLException;

/**
 * The secure Netty client via SSL. This is used for secret key exchange.
 */
public final class NettySecureRpcClient {
  /**  Share both the encoder and decoder with all the client pipelines. */
  private static final RPCMessageEncoder ENCODER = new RPCMessageEncoder();
  private static final RPCMessageDecoder DECODER = new RPCMessageDecoder();
  private static final KerberosSaslClientHandler KERBEROS_SASL_CLIENT_HANDLER =
      new KerberosSaslClientHandler();

  private static final ChannelType CHANNEL_TYPE =
      Configuration.getEnum(PropertyKey.USER_NETWORK_NETTY_CHANNEL, ChannelType.class);
  private static final Class<? extends Channel> CLIENT_CHANNEL_CLASS = NettyUtils
      .getClientChannelClass(CHANNEL_TYPE, false);

  /**
   * Reuse {@link EventLoopGroup} for all clients. Use daemon threads so the JVM is allowed to
   * shutdown even when daemon threads are alive. If number of worker threads is 0, Netty creates
   * (#processors * 2) threads by default.
   */
  private static final EventLoopGroup WORKER_GROUP = NettyUtils.createEventLoop(CHANNEL_TYPE,
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WORKER_THREADS), "netty-master-worker-%d",
      true);

  /** The maximum number of milliseconds to wait for a response from the server. */
  public static final long TIMEOUT_MS =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private NettySecureRpcClient() {} // prevent instantiation

  // TODO(chaomin): dedup the bootstrap creation logic with NettyClient
  /**
   * Creates and returns a new Netty client bootstrap for clients to connect to remote servers.
   *
   * @param address the remote server address
   * @return the new client {@link Bootstrap}
   * @throws SSLException when it fails to create a ssl context
   */
  public static Bootstrap createClientBootstrap(final InetSocketAddress address)
      throws SSLException {
    // TODO(chaomin): InsecureTrustManagerFactory does not verify the certificate. Ideally the
    // certificate should be provided and imported by the user.
    final SslContext sslCtx =
        SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

    final Bootstrap boot = new Bootstrap();

    boot.remoteAddress(address);
    boot.group(WORKER_GROUP).channel(CLIENT_CHANNEL_CLASS);
    boot.option(ChannelOption.SO_KEEPALIVE, true);
    boot.option(ChannelOption.TCP_NODELAY, true);
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    boot.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        // Use SSL for secure RPC.
        pipeline.addLast(sslCtx.newHandler(ch.alloc(), address.getHostName(), address.getPort()));
        pipeline.addLast(RPCMessage.createFrameDecoder());
        pipeline.addLast(ENCODER);
        pipeline.addLast(DECODER);
        if (Configuration.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE).equals(
            AuthType.KERBEROS.getAuthName())) {
          pipeline.addLast(KERBEROS_SASL_CLIENT_HANDLER);
        }
      }
    });
    return boot;
  }

  /**
   * Waits for the channel to be ready. If Kerberos security is enabled, waits until the channel
   * is authenticated.
   *
   * @param channel the input channel
   * @throws IOException if authentication failed
   */
  public static void waitForChannelReady(Channel channel) throws IOException {
    if (Configuration.get(alluxio.PropertyKey.SECURITY_AUTHENTICATION_TYPE)
        .equals(AuthType.KERBEROS.getAuthName())) {
      ChannelHandlerContext ctx = channel.pipeline().context(KerberosSaslClientHandler.class);
      if (ctx != null) {
        try {
          // Waits for the authentication result. Stop the process if authentication failed.
          if (!((KerberosSaslClientHandler) ctx.handler()).channelAuthenticated(ctx)) {
            throw new IOException("Sasl authentication is finished but failed.");
          }
        } catch (Exception e) {
          throw new IOException("Failed to authenticate", e);
        }
      }
    }
  }
}
