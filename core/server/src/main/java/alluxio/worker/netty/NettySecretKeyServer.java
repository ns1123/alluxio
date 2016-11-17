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

package alluxio.worker.netty;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.network.ChannelType;
import alluxio.util.network.NettyUtils;
import alluxio.worker.AlluxioWorkerService;

import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLException;

/**
 * A secure Netty server for secret key exchange.
 */
@NotThreadSafe
public final class NettySecretKeyServer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final ServerBootstrap mBootstrap;
  private final ChannelFuture mChannelFuture;
  private final SecretKeyServerHandler mSecretKeyServerHandler;

  /**
   * Creates a new instance of {@link NettySecretKeyServer}.
   *
   * @param address the server address
   * @param worker the Alluxio worker which contains the appropriate components to handle secret key
   *               exchange operations
   * @throws SSLException when it fails to create ssl context
   * @throws CertificateException when it fails to create certificate
   * @throws InterruptedException when the connection is interrupted
   */
  public NettySecretKeyServer(final InetSocketAddress address, final AlluxioWorkerService worker)
      throws SSLException, CertificateException, InterruptedException {
    SelfSignedCertificate ssc = new SelfSignedCertificate();
    SslContext sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
    mSecretKeyServerHandler = new SecretKeyServerHandler(Preconditions.checkNotNull(worker));

    mBootstrap = createBootstrap().childHandler(
        new SecretKeyServerInitializer(sslCtx, mSecretKeyServerHandler));
    mChannelFuture = mBootstrap.bind(address).sync();
  }

  /**
   * Gets the bind host of secret key server.
   *
   * @return the bind host
   */
  public String getBindHost() {
    // Return value of io.netty.channel.Channel.localAddress() must be down-cast into types like
    // InetSocketAddress to get detailed info such as port.
    return ((InetSocketAddress) mChannelFuture.channel().localAddress()).getHostString();
  }

  /**
   * Gets the port of secret key server.
   *
   * @return the port of secret key server
   */
  public int getPort() {
    // Return value of io.netty.channel.Channel.localAddress() must be down-cast into types like
    // InetSocketAddress to get detailed info such as port.
    return ((InetSocketAddress) mChannelFuture.channel().localAddress()).getPort();
  }

  /**
   * Closes Netty channel to shut down the secret key server.
   *
   * @throws IOException if fails to close Netty channel or EventLoopGroup
   */
  public void close() throws IOException {
    int quietPeriodSecs =
        Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD);
    int timeoutSecs = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT);

    // The following steps are needed to shut down the secret key server:
    //
    // 1) its channel needs to be closed
    // 2) its main EventLoopGroup needs to be shut down
    // 3) its child EventLoopGroup needs to be shut down
    //
    // Each of the above steps can time out. If 1) times out, we simply give up on closing the
    // channel. If 2) or 3) times out, the respective EventLoopGroup failed to shut down
    // gracefully and its shutdown is forced.

    boolean completed;
    completed =
        mChannelFuture.channel().close().awaitUninterruptibly(timeoutSecs, TimeUnit.SECONDS);
    if (!completed) {
      LOG.warn("Closing the channel timed out.");
    }
    completed =
        mBootstrap.group().shutdownGracefully(quietPeriodSecs, timeoutSecs, TimeUnit.SECONDS)
            .awaitUninterruptibly(timeoutSecs, TimeUnit.SECONDS);
    if (!completed) {
      LOG.warn("Forced group shutdown because graceful shutdown timed out.");
    }
    completed =
        mBootstrap.childGroup().shutdownGracefully(quietPeriodSecs, timeoutSecs, TimeUnit.SECONDS)
            .awaitUninterruptibly(timeoutSecs, TimeUnit.SECONDS);
    if (!completed) {
      LOG.warn("Forced child group shutdown because graceful shutdown timed out.");
    }
  }

  // TODO(chaomin): dedup the bootstrap creation logic with NettyDataServer
  private ServerBootstrap createBootstrap() {
    final ServerBootstrap boot = createBootstrapOfType(
        Configuration.getEnum(PropertyKey.WORKER_NETWORK_NETTY_CHANNEL, ChannelType.class));

    // use pooled buffers
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    boot.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    // set write buffer
    // this is the default, but its recommended to set it in case of change in future netty.
    boot.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
        (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_HIGH));
    boot.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
        (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_LOW));

    // more buffer settings on Netty socket option, one can tune them by specifying
    // properties, e.g.:
    // alluxio.worker.network.netty.backlog=50
    // alluxio.worker.network.netty.buffer.send=64KB
    // alluxio.worker.network.netty.buffer.receive=64KB
    if (Configuration.containsKey(PropertyKey.WORKER_NETWORK_NETTY_BACKLOG)) {
      boot.option(ChannelOption.SO_BACKLOG,
          Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BACKLOG));
    }
    if (Configuration.containsKey(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_SEND)) {
      boot.option(ChannelOption.SO_SNDBUF,
          (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_SEND));
    }
    if (Configuration.containsKey(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_RECEIVE)) {
      boot.option(ChannelOption.SO_RCVBUF,
          (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_RECEIVE));
    }
    return boot;
  }

  /**
   * Creates a default {@link io.netty.bootstrap.ServerBootstrap} where the channel and groups are
   * preset.
   *
   * @param type the channel type; current channel types supported are nio and epoll
   * @return an instance of {@code ServerBootstrap}
   */
  private ServerBootstrap createBootstrapOfType(final ChannelType type) {
    final ServerBootstrap boot = new ServerBootstrap();
    final int bossThreadCount = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);
    // If number of worker threads is 0, Netty creates (#processors * 2) threads by default.
    final int workerThreadCount =
        Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS);
    final EventLoopGroup bossGroup =
        NettyUtils.createEventLoop(type, bossThreadCount, "secret-key-server-boss-%d", false);
    final EventLoopGroup workerGroup =
        NettyUtils.createEventLoop(type, workerThreadCount, "secret-key-server-worker-%d", false);

    final Class<? extends ServerChannel> socketChannelClass =
        NettyUtils.getServerChannelClass(type);
    boot.group(bossGroup, workerGroup).channel(socketChannelClass);

    return boot;
  }
}
