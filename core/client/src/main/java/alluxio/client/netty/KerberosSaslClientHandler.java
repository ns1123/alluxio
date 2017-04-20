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

import static com.google.common.base.Preconditions.checkState;

import alluxio.netty.NettyAttributes;
import alluxio.network.protocol.RPCProtoMessage;

import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;
import alluxio.util.proto.ProtoUtils;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutionException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This handles all the messages received by the client channel which is secured by Sasl Client,
 * with Kerberos Login.
 */
@ChannelHandler.Sharable
@ThreadSafe
public final class KerberosSaslClientHandler extends SimpleChannelInboundHandler<RPCProtoMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosSaslClientHandler.class);
  private static final AttributeKey<KerberosSaslNettyClient> CLIENT_KEY =
      AttributeKey.valueOf("CLIENT_KEY");
  private static final AttributeKey<SettableFuture<Boolean>> AUTHENTICATED_KEY =
      AttributeKey.valueOf("AUTHENTICATED_KEY");

  /**
   * The default constructor.
   */
  public KerberosSaslClientHandler() {}

  /**
   * Waits to receive the result whether the channel is authenticated.
   *
   * @param ctx the channel handler context
   * @return true the channel is authenticated successfully, false otherwise
   * @throws ExecutionException if the task completed with an error
   * @throws InterruptedException the current thread was interrupted before
   *                              or during the call
   */
  public boolean channelAuthenticated(final ChannelHandlerContext ctx)
      throws ExecutionException, InterruptedException {
    return ctx.attr(AUTHENTICATED_KEY).get().get();
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    ctx.attr(AUTHENTICATED_KEY).setIfAbsent(SettableFuture.<Boolean>create());
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    ctx.attr(CLIENT_KEY).setIfAbsent(new KerberosSaslNettyClient(
        ctx.channel().attr(NettyAttributes.HOSTNAME_KEY).get()));
    ctx.writeAndFlush(getInitialChallenge(ctx));
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCProtoMessage msg)
      throws IOException {
    // Only handle SASL_MESSAGE
    if (msg.getMessage().getType() != ProtoMessage.Type.SASL_MESSAGE) {
      ctx.fireChannelRead(msg);
      return;
    }

    KerberosSaslNettyClient client = ctx.attr(CLIENT_KEY).get();
    SettableFuture<Boolean> authenticated = ctx.attr(AUTHENTICATED_KEY).get();
    Preconditions.checkNotNull(client);
    Preconditions.checkNotNull(authenticated);

    Protocol.SaslMessage message = msg.getMessage().getMessage();

    switch (message.getState()) {
      case CHALLENGE:
        byte[] challengeResponse = client.response(message.getToken().toByteArray());
        if (challengeResponse == null) {
          checkState(client.isComplete());
          return;
        }
        LOG.debug("Response to server token with length: {}", challengeResponse.length);
        Protocol.SaslMessage response =
            ProtoUtils.setToken(Protocol.SaslMessage.newBuilder()
                .setState(Protocol.SaslMessage.SaslState.RESPONSE), challengeResponse).build();
        ctx.writeAndFlush(response);
        break;
      case SUCCESS:
        checkState(client.isComplete());
        LOG.debug("Sasl authentication is completed.");
        ctx.pipeline().remove(KerberosSaslClientHandler.class);
        authenticated.set(true);
        break;
      default:
        throw new IOException("Abort: Unexpected SASL message with state: " + message.getState());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    // Propagate the exception caught to authentication result.
    ctx.attr(AUTHENTICATED_KEY).get().setException(cause);
    ctx.close();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    SettableFuture<Boolean> future = ctx.attr(AUTHENTICATED_KEY).get();
    if (!future.isDone()) {
      future.setException(new ClosedChannelException());
    }
    ctx.fireChannelUnregistered();
  }

  /**
   * Gets the initial Sasl challenge.
   *
   * @return the Sasl challenge as an {@link RPCProtoMessage}
   * @throws Exception if failed to create the initial challenge
   */
  private RPCProtoMessage getInitialChallenge(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("Going to initiate Kerberos negotiations.");
    byte[] initialChallenge = ctx.attr(CLIENT_KEY).get().response(new byte[0]);
    LOG.debug("Sending initial challenge, length : {} context : {}", initialChallenge.length,
        initialChallenge);
    Protocol.SaslMessage message =
        ProtoUtils.setToken(
            Protocol.SaslMessage.newBuilder().setState(Protocol.SaslMessage.SaslState.INITIATE),
            initialChallenge).build();
    return new RPCProtoMessage(new ProtoMessage(message), null);
  }
}
