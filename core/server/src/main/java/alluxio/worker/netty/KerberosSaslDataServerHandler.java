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

import static com.google.common.base.Preconditions.checkState;

import alluxio.Constants;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.RPCSaslCompleteResponse;
import alluxio.network.protocol.RPCSaslTokenRequest;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.sasl.SaslException;

/**
 * The Netty server handler secured by Sasl, with Kerberos Login.
 */
@ChannelHandler.Sharable
@NotThreadSafe
public class KerberosSaslDataServerHandler extends SimpleChannelInboundHandler<RPCMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private KerberosSaslNettyServer mServer;

  /**
   * The default constructor.
   *
   * @throws SaslException if failed to create a Sasl netty server
   */
  public KerberosSaslDataServerHandler() throws SaslException {
    mServer = new KerberosSaslNettyServer();
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCMessage msg)
      throws IOException, SaslException {
    Channel channel = ctx.channel();

    if (msg.getType() == RPCMessage.Type.RPC_SASL_TOKEN_REQUEST) {
      assert msg instanceof RPCSaslTokenRequest;
      RPCSaslTokenRequest req = (RPCSaslTokenRequest) msg;
      req.validate();

      LOG.debug("Got Sasl token request.");

      ByteBuffer payload = req.getPayloadDataBuffer().getReadOnlyByteBuffer();
      int numBytes = (int) req.getPayloadDataBuffer().getLength();
      byte[] token = new byte[numBytes];
      payload.get(token);
      byte[] responseBytes = mServer.response(token);

      if (responseBytes != null) {
        // Send response to client.
        RPCSaslTokenRequest saslTokenMessageRequest = new RPCSaslTokenRequest(responseBytes);
        channel.writeAndFlush(saslTokenMessageRequest);
        return;
      }

      checkState(mServer.isComplete());
      // If authentication of client is completed, send a complete message to the client.
      LOG.debug("Sasl authentication is completed for Netty client.");
      channel.writeAndFlush(new RPCSaslCompleteResponse(RPCResponse.Status.SUCCESS));
      LOG.debug("Removing KerberosSaslDataServerHandler from pipeline as Sasl authentication"
          + " is completed.");
      ctx.pipeline().remove(this);
    } else {
      throw new IOException("Receiving non-Sasl message before authentication is completed. "
          + "Aborting.");
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }
}
