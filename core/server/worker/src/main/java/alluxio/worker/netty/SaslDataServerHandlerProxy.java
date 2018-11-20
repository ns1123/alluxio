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

import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.security.CapabilityCache;

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The Netty server handler that forwards Digest and Kerberos logins.
 */
@NotThreadSafe
public class SaslDataServerHandlerProxy extends SimpleChannelInboundHandler<RPCProtoMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(SaslDataServerHandlerProxy.class);

  private CapabilityCache mCapabilityCache = null;

  /**
   * The default constructor.
   * @param capabilityCache the capability cache for handling digest logins
   */
  public SaslDataServerHandlerProxy(CapabilityCache capabilityCache) {
    mCapabilityCache = capabilityCache;
  }

  /**
   * Initializes the {@link KerberosSaslDataServerHandler} when the handler is registered.
   */
  @Override
  public void channelRegistered(ChannelHandlerContext ctx) {
    // Prevents default implementation from forwarding the register event until authenticated.
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCProtoMessage msg)
      throws Exception {
    // Only handle SASL_MESSAGE
    if (!msg.getMessage().isSaslMessage()) {
      ctx.fireChannelRead(msg);
      return;
    }

    Protocol.SaslMessage message = msg.getMessage().asSaslMessage();
    Preconditions.checkState(message.getState() == Protocol.SaslMessage.SaslState.INITIATE);

    SimpleChannelInboundHandler<RPCProtoMessage> authHandler = null;
    // Determine underlying channel handler based on initiate request:
    // * Capability authentication initiates with empty token
    // * Kerberos sends a valid token
    if (message.getToken().size() == 0) {
      authHandler = new CapabilityAuthenticationSaslDataServerHandler(mCapabilityCache);
    } else {
      authHandler = new KerberosSaslDataServerHandler();
    }
    LOG.debug("Selected {} for handling authentication.", authHandler.getClass().getName());
    // Replace proxy handler with the selected handler.
    ctx.pipeline().replace(this, authHandler.getClass().getName(), authHandler);
    // Replay INITIATE stage to the selected handler.
    authHandler.channelRegistered(ctx);
    authHandler.channelRead(ctx, msg);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request.", cause);
    ctx.close();
  }
}
