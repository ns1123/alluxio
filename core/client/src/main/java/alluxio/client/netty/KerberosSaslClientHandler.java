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
 * This handles all the messages received by the client channel which is secured by Sasl Client,
 * with Kerberos Login.
 */
@ChannelHandler.Sharable
@NotThreadSafe
public final class KerberosSaslClientHandler extends SimpleChannelInboundHandler<RPCMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private KerberosSaslNettyClient mClient;

  /**
   * The default constructor.
   *
   * @throws SaslException if failed to create a Sasl netty client
   */
  public KerberosSaslClientHandler() throws SaslException {
    try {
      mClient = new KerberosSaslNettyClient();
    } catch (SaslException e) {
      LOG.error("Failed to start KerberosSaslNettyClient, stopping KerberoSaslClientHandler");
    }
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) {
    // register the newly established channel
    Channel channel = ctx.channel();

    try {
      if (mClient == null) {
        LOG.debug("Creating KerberosSaslNettyClient now.");
        mClient = new KerberosSaslNettyClient();
      }
      LOG.debug("Going to initiate Kerberos negotiations.");
      byte[] initialChallenge = mClient.response(new byte[0]);
      LOG.debug("Sending initial challenge, length : {} context : {}", initialChallenge.length,
          initialChallenge);
      channel.write(new RPCSaslTokenRequest(initialChallenge));
    } catch (Exception e) {
      LOG.error("Failed to authenticate with server due to error: ", e);
    }
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCMessage msg)
      throws IOException {
    Channel channel = ctx.channel();

    if (mClient == null) {
      throw new IOException("mClient was unexpectedly null.");
    }

    if (msg instanceof RPCSaslCompleteResponse) {
      RPCSaslCompleteResponse response = ((RPCSaslCompleteResponse) msg);
      if (response.getStatus() == RPCResponse.Status.SUCCESS) {
        if (!mClient.isComplete()) {
          String err = "Server said the Sasl is completed, but the client is not completed yet.";
          LOG.error(err);
          throw new IOException(err);
        }
        LOG.debug("Server has sent us the SaslComplete message. Allowing normal work to proceed.");
        ctx.pipeline().remove(this);
      } else {
        String err = "Failed to authenticate with Sasl.";
        LOG.error(err);
        throw new IOException(err);
      }
    } else if (msg instanceof RPCSaslTokenRequest) {
      // Generate Sasl if the request is not null.
      ByteBuffer payload = msg.getPayloadDataBuffer().getReadOnlyByteBuffer();
      int numBytes = payload.remaining();
      byte[] token = new byte[numBytes];
      payload.get(token, 0, numBytes);
      byte[] responseToServer = mClient.response(token);
      if (responseToServer == null) {
        LOG.debug("Response to server is null: authentication should now be complete.");
        if (!mClient.isComplete()) {
          LOG.warn("Generated a null response, but authentication is not complete.");
          throw new IOException("Our response to the server is null, but the client show that"
              + " it is not authenticated.");
        }
        return;
      }
      LOG.debug("Response to server token has length: {}", responseToServer.length);
      RPCSaslTokenRequest saslResponse = new RPCSaslTokenRequest(responseToServer);
      channel.writeAndFlush(saslResponse);
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
