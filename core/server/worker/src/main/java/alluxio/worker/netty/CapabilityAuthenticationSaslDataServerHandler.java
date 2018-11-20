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

import alluxio.worker.security.CapabilityCache;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.sasl.SaslException;

/**
 * The Netty server handler secured by Sasl, with Digest Login.
 */
@NotThreadSafe
public class CapabilityAuthenticationSaslDataServerHandler
    extends SimpleChannelInboundHandler<RPCProtoMessage> {
  private static final Logger LOG =
          LoggerFactory.getLogger(CapabilityAuthenticationSaslDataServerHandler.class);

  private DigestSaslNettyServer mServer = null;
  private CapabilityCache mCapabilityCache;

  /**
   * @param capabilityCache Capability cache to use for password generation
   */
  public CapabilityAuthenticationSaslDataServerHandler(CapabilityCache capabilityCache) {
    mCapabilityCache = capabilityCache;
  }

  /**
   * Initializes the {@link CapabilityAuthenticationSaslDataServerHandler} when the handler is
   * registered.
   *
   * @throws SaslException if failed to create a Sasl netty server
   */
  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws SaslException {
    LOG.debug("Channel ready for handling Sasl Digest authentication request.");
    mServer = new DigestSaslNettyServer(ctx.channel(),
      mCapabilityCache.getActiveCapabilityKeys());
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCProtoMessage msg)
          throws IOException, SaslException {
    Preconditions.checkNotNull(mServer);
    // Only handle SASL_MESSAGE
    if (!msg.getMessage().isSaslMessage()) {
      ctx.fireChannelRead(msg);
      return;
    }
    Protocol.SaslMessage message = msg.getMessage().asSaslMessage();
    switch (message.getState()) {
      case INITIATE:
        LOG.debug("Got Sasl initiate request.");
        byte[] initialChallenge = mServer.response(new byte[0]);
        LOG.debug("Sending initial challenge, length : {} context : {}", initialChallenge.length,
                initialChallenge);
        Protocol.SaslMessage response = ProtoUtils.setToken(
                Protocol.SaslMessage.newBuilder().setState(Protocol.SaslMessage.SaslState.CHALLENGE),
                initialChallenge).build();
        ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(response), null));
        break;
      case CHALLENGE:
        LOG.debug("Got Sasl token request.");
        byte[] challenge = mServer.response(message.getToken().toByteArray());
        // Digest never response null for a challenge
        Preconditions.checkNotNull(challenge);
        if (!mServer.isComplete()) { // more challenges
          LOG.debug("Sending initial challenge, length : {} context : {}", challenge.length,
                  challenge);
          Protocol.SaslMessage challange = ProtoUtils.setToken(
                  Protocol.SaslMessage.newBuilder().setState(Protocol.SaslMessage.SaslState.CHALLENGE),
                  challenge).build();
          ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(challange), null));
        } else { // Authentication is completed for client
          LOG.debug("Sasl authentication with Digest is completed for Netty client.");
          Protocol.SaslMessage challange = ProtoUtils.setToken(
                  Protocol.SaslMessage.newBuilder().setState(Protocol.SaslMessage.SaslState.SUCCESS),
                  challenge).build();
          ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(challange), null));
          ctx.pipeline().remove(this);
          ctx.fireChannelRegistered();
        }
        break;
      default:
        // The server handles initiate and challenge, but should never receive success.
        throw new IOException("Abort: Unexpected SASL message with state: " + message.getState());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }
}
