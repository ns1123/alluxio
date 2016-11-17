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

import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;

/**
 * Creates a newly configured {@link ChannelPipeline} for a new secure channel.
 */
public class SecretKeyServerInitializer extends ChannelInitializer<SocketChannel> {
  private final SslContext mSslCtx;
  private final SecretKeyServerHandler mHandler;

  /**
   * Creates a new {@link SecretKeyServerInitializer} instance with given ssl context.
   *
   * @param sslCtx the ssl context
   * @param handler the secret key server handler
   */
  public SecretKeyServerInitializer(SslContext sslCtx, SecretKeyServerHandler handler) {
    mSslCtx = sslCtx;
    mHandler = handler;
  }

  @Override
  public void initChannel(SocketChannel channel) throws Exception {
    ChannelPipeline pipeline = channel.pipeline();

    if (mSslCtx != null) {
      pipeline.addLast(mSslCtx.newHandler(channel.alloc()));
    }
    pipeline.addLast("nioChunkedWriter", new ChunkedWriteHandler());
    pipeline.addLast("frameDecoder", RPCMessage.createFrameDecoder());
    pipeline.addLast("RPCMessageDecoder", new RPCMessageDecoder());
    pipeline.addLast("RPCMessageEncoder", new RPCMessageEncoder());
    if (alluxio.Configuration.get(alluxio.PropertyKey.SECURITY_AUTHENTICATION_TYPE).equals(
        alluxio.security.authentication.AuthType.KERBEROS.getAuthName())) {
      pipeline.addLast(new KerberosSaslDataServerHandler());
    }
    pipeline.addLast(mHandler);
  }
}
