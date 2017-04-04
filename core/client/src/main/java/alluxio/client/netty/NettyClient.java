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
import alluxio.util.network.NettyUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Shared configuration and methods for the Netty client.
 */
@ThreadSafe
public final class NettyClient {
  private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

  /**  Share both the encoder and decoder with all the client pipelines. */
  private static final RPCMessageEncoder ENCODER = new RPCMessageEncoder();
  private static final RPCMessageDecoder DECODER = new RPCMessageDecoder();
<<<<<<< HEAD
  // ALLUXIO CS ADD
  private static final KerberosSaslClientHandler KERBEROS_SASL_CLIENT_HANDLER =
      new KerberosSaslClientHandler();
  // ALLUXIO CS END
  private static final boolean PACKET_STREAMING_ENABLED =
      Configuration.getBoolean(PropertyKey.USER_PACKET_STREAMING_ENABLED);
||||||| merged common ancestors
  private static final boolean PACKET_STREAMING_ENABLED =
      Configuration.getBoolean(PropertyKey.USER_PACKET_STREAMING_ENABLED);
=======
>>>>>>> FETCH_HEAD

  private static final ChannelType CHANNEL_TYPE = getChannelType();
  private static final Class<? extends SocketChannel> CLIENT_CHANNEL_CLASS = NettyUtils
      .getClientChannelClass(CHANNEL_TYPE);
  /**
   * Reuse {@link EventLoopGroup} for all clients. Use daemon threads so the JVM is allowed to
   * shutdown even when daemon threads are alive. If number of worker threads is 0, Netty creates
   * (#processors * 2) threads by default.
   */
  private static final EventLoopGroup WORKER_GROUP = NettyUtils.createEventLoop(CHANNEL_TYPE,
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WORKER_THREADS), "netty-client-worker-%d",
      true);

  /** The maximum number of milliseconds to wait for a response from the server. */
  public static final long TIMEOUT_MS =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private NettyClient() {} // prevent instantiation

  /**
   * Creates and returns a new Netty client bootstrap for clients to connect to remote servers.
   *
   * @return the new client {@link Bootstrap}
   */
  public static Bootstrap createClientBootstrap() {
    final Bootstrap boot = new Bootstrap();

    boot.group(WORKER_GROUP).channel(CLIENT_CHANNEL_CLASS);
    boot.option(ChannelOption.SO_KEEPALIVE, true);
    boot.option(ChannelOption.TCP_NODELAY, true);
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    if (CHANNEL_TYPE == ChannelType.EPOLL) {
      boot.option(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
    }

    boot.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(RPCMessage.createFrameDecoder());
        pipeline.addLast(ENCODER);
        pipeline.addLast(DECODER);
        // ALLUXIO CS ADD
        if (Configuration.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE).equals(
            alluxio.security.authentication.AuthType.KERBEROS.getAuthName())) {
          pipeline.addLast(KERBEROS_SASL_CLIENT_HANDLER);
        }
        // ALLUXIO CS END
      }
    });

    return boot;
  }
  // ALLUXIO CS ADD
  /**
   * Waits for the channel to be ready. If Kerberos security is enabled, waits until the channel
   * is authenticated.
   *
   * @param channel the input channel
   * @throws java.io.IOException if authentication failed
   */
  public static void waitForChannelReady(io.netty.channel.Channel channel)
      throws java.io.IOException {
    if (alluxio.Configuration.get(alluxio.PropertyKey.SECURITY_AUTHENTICATION_TYPE)
        .equals(alluxio.security.authentication.AuthType.KERBEROS.getAuthName())) {
      io.netty.channel.ChannelHandlerContext ctx =
          channel.pipeline().context(KerberosSaslClientHandler.class);
      if (ctx != null) {
        try {
          // Waits for the authentication result. Stop the process if authentication failed.
          if (!((KerberosSaslClientHandler) ctx.handler()).channelAuthenticated(ctx)) {
            throw new java.io.IOException("Sasl authentication is finished but failed.");
          }
        } catch (Exception e) {
          throw new java.io.IOException("Failed to authenticate", e);
        }
      }
    }
  }
  // ALLUXIO CS END

  /**
   * Note: Packet streaming requires {@link io.netty.channel.epoll.EpollMode} to be set to
   * LEVEL_TRIGGERED which is not supported in netty versions < 4.0.26.Final. Without shading
   * netty in Alluxio, we cannot use epoll.
   *
   * @return {@link ChannelType} to use
   */
  private static ChannelType getChannelType() {
    try {
      EpollChannelOption.class.getField("EPOLL_MODE");
    } catch (Throwable e) {
      LOG.warn("EPOLL_MODE is not supported in netty with version < 4.0.26.Final.");
      return ChannelType.NIO;
    }
    return Configuration.getEnum(PropertyKey.USER_NETWORK_NETTY_CHANNEL, ChannelType.class);
  }
}
