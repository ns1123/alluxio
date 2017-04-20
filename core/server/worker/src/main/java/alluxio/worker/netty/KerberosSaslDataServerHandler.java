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
 * The Netty server handler secured by Sasl, with Kerberos Login.
 */
@NotThreadSafe
public class KerberosSaslDataServerHandler extends SimpleChannelInboundHandler<RPCProtoMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosSaslDataServerHandler.class);

  private KerberosSaslNettyServer mServer = null;

  /**
   * The default constructor.
   *
   */
  public KerberosSaslDataServerHandler() {}

  /**
   * Initializes the {@link KerberosSaslDataServerHandler} when the handler is registered.
   * @throws SaslException if failed to create a Sasl netty server
   */
  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws SaslException {
    mServer = new KerberosSaslNettyServer(ctx.channel());
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCProtoMessage msg)
      throws IOException, SaslException {
    Preconditions.checkNotNull(mServer);
    // Only handle SASL_MESSAGE
    if (msg.getMessage().getType() != ProtoMessage.Type.SASL_MESSAGE) {
      ctx.fireChannelRead(msg);
      return;
    }

    Protocol.SaslMessage message = msg.getMessage().getMessage();

    if (message.getState().equals(Protocol.SaslMessage.SaslState.INITIATE)) {
      LOG.debug("Got Sasl token request.");
      byte[] challenge = mServer.response(message.getToken().toByteArray());
      if (challenge != null) { // more challenges
        Protocol.SaslMessage response =
            ProtoUtils.setToken(
                Protocol.SaslMessage.newBuilder()
                    .setState(Protocol.SaslMessage.SaslState.CHALLENGE), challenge).build();
        ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(response), null));
        return;
      } else { // no more challenges
        LOG.debug("Sasl authentication is completed for Netty client.");
        Protocol.SaslMessage response =
            Protocol.SaslMessage.newBuilder().setState(Protocol.SaslMessage.SaslState.SUCCESS)
                .build();
        ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(response), null));
        LOG.debug("Removing KerberosSaslDataServerHandler from pipeline as Sasl authentication"
            + " is completed.");
        ctx.pipeline().remove(this);
        ctx.fireChannelRegistered();
      }
    } else {
      throw new IOException("Abort: Unexpected SASL message with state: " + message.getState());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }
}
