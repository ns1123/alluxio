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

import alluxio.netty.NettyAttributes;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.security.CapabilityProto;
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
 * with Digest authentication.
 */
@ChannelHandler.Sharable
@ThreadSafe
public final class CapabilityAuthenticationSaslClientHandler
    extends SimpleChannelInboundHandler<RPCProtoMessage> {
  private static final Logger LOG =
      LoggerFactory.getLogger(CapabilityAuthenticationSaslClientHandler.class);
  private static final AttributeKey<DigestSaslNettyClient> CLIENT_KEY =
      AttributeKey.valueOf("CLIENT_KEY");
  private static final AttributeKey<SettableFuture<Boolean>> AUTHENTICATED_KEY =
      AttributeKey.valueOf("AUTHENTICATED_KEY");
  private final CapabilityProto.Capability mChannelCapability;

  /**
   * @param channelCapability the capability that will be used as credentials for authentication
   */
  public CapabilityAuthenticationSaslClientHandler(CapabilityProto.Capability channelCapability) {
    mChannelCapability = channelCapability;
  }

  /**
   * Waits to receive the result whether the channel is authenticated.
   *
   * @param ctx the channel handler context
   * @return true the channel is authenticated successfully, false otherwise
   * @throws ExecutionException if the task completed with an error
   * @throws InterruptedException the current thread was interrupted before or during the call
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
    ctx.attr(CLIENT_KEY).setIfAbsent(new DigestSaslNettyClient(mChannelCapability,
        ctx.channel().attr(NettyAttributes.HOSTNAME_KEY).get()));
    initiateDigestAuthentication(ctx);
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCProtoMessage msg)
      throws IOException {
    // Only handle SASL_MESSAGE
    if (!msg.getMessage().isSaslMessage()) {
      ctx.fireChannelRead(msg);
      return;
    }

    DigestSaslNettyClient client = ctx.attr(CLIENT_KEY).get();
    SettableFuture<Boolean> authenticated = ctx.attr(AUTHENTICATED_KEY).get();
    Preconditions.checkNotNull(client);
    Preconditions.checkNotNull(authenticated);

    Protocol.SaslMessage message = msg.getMessage().asSaslMessage();
    switch (message.getState()) {
      case CHALLENGE:
        byte[] challengeResponse = client.response(message.getToken().toByteArray());
        LOG.debug("Response to server token with length: {}", challengeResponse.length);
        Protocol.SaslMessage response = ProtoUtils.setToken(
            Protocol.SaslMessage.newBuilder().setState(Protocol.SaslMessage.SaslState.CHALLENGE),
            challengeResponse).build();
        ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(response), null));
        break;
      case SUCCESS:
        byte[] finalUpdateResult = client.response(message.getToken().toByteArray());
        Preconditions.checkState(finalUpdateResult == null);
        Preconditions.checkState(client.isComplete());
        LOG.debug("Sasl authentication is completed.");
        authenticated.set(true);
        ctx.pipeline().remove(CapabilityAuthenticationSaslClientHandler.class);
        break;
      default:
        // The client handles challenge and success, but should never receive initiate.
        throw new IOException("Abort(Digest): Unexpected SASL message with state: " + message.getState());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request with Digest. {}", cause.toString());
    // Propagate the exception caught to authentication result.
    SettableFuture<Boolean> authenticated = ctx.attr(AUTHENTICATED_KEY).get();
    if (authenticated != null) {
      authenticated.setException(cause);
    }
    ctx.close();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    SettableFuture<Boolean> authenticated = ctx.attr(AUTHENTICATED_KEY).get();
    if (authenticated != null && !authenticated.isDone()) {
      authenticated.setException(new ClosedChannelException());
    }
    ctx.fireChannelUnregistered();
  }

  private void initiateDigestAuthentication(ChannelHandlerContext ctx) {
    Protocol.SaslMessage initiateProtoMsg =
        Protocol.SaslMessage.newBuilder().setState(Protocol.SaslMessage.SaslState.INITIATE).build();
    ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(initiateProtoMsg), null));
  }
}
