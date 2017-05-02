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

import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.Status;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.security.Key;
import alluxio.security.capability.CapabilityKey;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The secure Netty server handler for secure secret key exchange.
 */
@ChannelHandler.Sharable
@NotThreadSafe
public class SecretKeyServerHandler extends SimpleChannelInboundHandler<RPCProtoMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(SecretKeyServerHandler.class);

  private final WorkerProcess mWorkerProcess;

  /**
   * Creates a new {@link SecretKeyServerHandler} instance with alluxio worker service.
   *
   * @param workerProcess the Alluxio worker process
   */
  public SecretKeyServerHandler(final WorkerProcess workerProcess) {
    Preconditions.checkNotNull(workerProcess, "workerProcess");
    mWorkerProcess = workerProcess;
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
  public void channelRead0(ChannelHandlerContext ctx, final RPCProtoMessage msg) {
    // Only handle SECRET_KEY
    if (msg.getType() != RPCMessage.Type.RPC_SECRET_KEY) {
      ctx.fireChannelRead(msg);
    }

    Key.SecretKey request = msg.getMessage().getMessage();
    byte[] secretKey = request.getSecretKey().toByteArray();

    if (secretKey.length == 0) {
      RPCProtoMessage error = RPCProtoMessage
          .createResponse(new InvalidArgumentException("Received secret key length is 0."));
      ctx.channel().writeAndFlush(error).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      return;
    }

    switch (request.getKeyType()) {
      case CAPABILITY:
        CapabilityKey key;
        try {
          key = new CapabilityKey(request.getKeyId(), request.getExpirationTimeMs(), secretKey);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
          RPCProtoMessage error = RPCProtoMessage.createResponse(new InvalidArgumentException(e));
          ctx.channel().writeAndFlush(error).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
          return;
        }
        Preconditions.checkNotNull(mWorkerProcess.getWorker(BlockWorker.class));
        mWorkerProcess.getWorker(BlockWorker.class).getCapabilityCache().setCapabilityKey(key);
        LOG.debug("Received secret key, id {}", key.getKeyId());
        // Send the secret key write response.
        ctx.channel().writeAndFlush(RPCProtoMessage.createOkResponse(null));
        break;
      default:
        RPCProtoMessage error =
            RPCProtoMessage.createResponse(Status.INVALID_ARGUMENT,
                "Unknown secret key type: " + request.getKeyType(), null);
        ctx.channel().writeAndFlush(error).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }
}
