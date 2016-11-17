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

import alluxio.Constants;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.RPCSecretKeyWriteResponse;
import alluxio.worker.AlluxioWorkerService;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The secure Netty server handler for secure secret key exchange.
 */
public class SecretKeyServerHandler extends SimpleChannelInboundHandler<RPCMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final AlluxioWorkerService mWorker;

  /**
   * Creates a new {@link SecretKeyServerHandler} instance with alluxio worker service.
   *
   * @param worker the alluxio worker
   */
  public SecretKeyServerHandler(final AlluxioWorkerService worker) {
    Preconditions.checkNotNull(worker, "worker");
    mWorker = worker;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ctx.pipeline().get(SslHandler.class).handshakeFuture().addListener(
        new GenericFutureListener<Future<Channel>>() {
          @Override
          public void operationComplete(Future<Channel> future) throws Exception {
            LOG.debug("Channel active operation completed");
          }
        });
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, final RPCMessage msg) throws Exception {
    // TODO(chaomin): store the imported key in worker instead of a dummy op here.
    mWorker.getAddress();
    // Send the secret key write response.
    ctx.channel().writeAndFlush(new RPCSecretKeyWriteResponse(RPCResponse.Status.SUCCESS));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
}
