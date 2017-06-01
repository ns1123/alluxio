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
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Shared configuration and methods for the Netty client.
 */
@ThreadSafe
public final class NettyClient {
  /**  Share both the encoder and decoder with all the client pipelines. */
  private static final RPCMessageEncoder ENCODER = new RPCMessageEncoder();
  private static final RPCMessageDecoder DECODER = new RPCMessageDecoder();
  // ALLUXIO CS ADD
  private static final KerberosSaslClientHandler KERBEROS_SASL_CLIENT_HANDLER =
      new KerberosSaslClientHandler();
  // ALLUXIO CS END

  /**
   * Reuse {@link EventLoopGroup} for all clients. Use daemon threads so the JVM is allowed to
   * shutdown even when daemon threads are alive. If number of worker threads is 0, Netty creates
   * (#processors * 2) threads by default.
   */
  private static final EventLoopGroup WORKER_GROUP = NettyUtils
      .createEventLoop(NettyUtils.CHANNEL_TYPE,
          Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WORKER_THREADS),
          "netty-client-worker-%d", true);

  private NettyClient() {} // prevent instantiation

  /**
   * Creates and returns a new Netty client bootstrap for clients to connect to remote servers.
   *
   * @param address the socket address
   * @return the new client {@link Bootstrap}
   */
  public static Bootstrap createClientBootstrap(SocketAddress address) {
    final Bootstrap boot = new Bootstrap();

    boot.group(WORKER_GROUP).channel(NettyUtils
        .getClientChannelClass(NettyUtils.CHANNEL_TYPE, !(address instanceof InetSocketAddress)));
    boot.option(ChannelOption.SO_KEEPALIVE, true);
    boot.option(ChannelOption.TCP_NODELAY, true);
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    if (NettyUtils.CHANNEL_TYPE == ChannelType.EPOLL) {
      boot.option(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
    }

    // ALLUXIO CS REMOVE
    // // After 10 missed heartbeat attempts and no write activity, the server will close the channel.
    // final long timeoutMs = Configuration.getLong(PropertyKey.NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS);
    // final long heartbeatPeriodMs = Math.max(timeoutMs / 10, 1);
    //
    // ALLUXIO CS END
    boot.handler(new ChannelInitializer<Channel>() {
      @Override
      public void initChannel(Channel ch) throws Exception {
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
        // ALLUXIO CS REMOVE
        // pipeline.addLast(new IdleStateHandler(0, heartbeatPeriodMs, 0, TimeUnit.MILLISECONDS));
        // pipeline.addLast(new IdleWriteHandler());
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
      throws alluxio.exception.status.AlluxioStatusException {
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
          throw alluxio.exception.status.AlluxioStatusException.fromThrowable(e);
        }
      }
    }
    // After 10 missed heartbeat attempts and no write activity, the server will close the channel.
    final long timeoutMs = Configuration.getLong(PropertyKey.NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS);
    final long heartbeatPeriodMs = Math.max(timeoutMs / 10, 1);
    channel.pipeline()
        .addLast(new IdleStateHandler(0, heartbeatPeriodMs, 0, TimeUnit.MILLISECONDS));
    channel.pipeline().addLast(new IdleWriteHandler());
  }
  // ALLUXIO CS END
}
