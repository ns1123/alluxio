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

import alluxio.Constants;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.RPCSaslCompleteResponse;
import alluxio.network.protocol.RPCSaslTokenRequest;

import com.google.common.base.Preconditions;
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
    mClient = new KerberosSaslNettyClient();
  }

  /**
   * Gets the initial Sasl challenge.
   *
   * @returns the Sasl challenge as {@link RPCSaslTokenRequest}
   * @throws Exception if failed to create the initial challenge
   */
  public RPCSaslTokenRequest getInitialChallenge() throws Exception {
    LOG.debug("Going to initiate Kerberos negotiations.");
    byte[] initialChallenge = mClient.response(new byte[0]);
    LOG.debug("Sending initial challenge, length : {} context : {}", initialChallenge.length,
        initialChallenge);
    return new RPCSaslTokenRequest(initialChallenge);
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCMessage msg)
      throws IOException {
    Channel channel = ctx.channel();

    Preconditions.checkNotNull(mClient);

    switch (msg.getType()) {
      case RPC_SASL_COMPLETE_RESPONSE:
        assert msg instanceof RPCSaslCompleteResponse;
        RPCSaslCompleteResponse response = (RPCSaslCompleteResponse) msg;
        if (response.getStatus() == RPCResponse.Status.SUCCESS) {
          checkState(mClient.isComplete());
          LOG.debug("Sasl authentication is completed.");
          ctx.pipeline().remove(this);
        } else {
          throw new IOException("Failed to authenticate.");
        }
        break;
      case RPC_SASL_TOKEN_REQUEST:
        // Generate Sasl response if the request is not null.
        ByteBuffer payload = msg.getPayloadDataBuffer().getReadOnlyByteBuffer();
        byte[] token = new byte[payload.remaining()];
        payload.get(token);
        byte[] responseToServer = mClient.response(token);
        if (responseToServer == null) {
          checkState(mClient.isComplete());
          return;
        }
        LOG.debug("Response to server token with length: {}", responseToServer.length);
        RPCSaslTokenRequest saslResponse = new RPCSaslTokenRequest(responseToServer);
        channel.writeAndFlush(saslResponse);
        break;
      default:
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
