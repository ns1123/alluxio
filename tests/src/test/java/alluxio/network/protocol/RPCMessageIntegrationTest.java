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

package alluxio.network.protocol;

import alluxio.PropertyKey;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.testutils.network.MessageSavingHandler;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * This tests the encoding and decoding of RPCMessage's. This is done by setting up a simple
 * client/server bootstrap connection, and writing messages on the client side, and verifying it on
 * the server side. In this case, the server simply stores the message received, and does not reply
 * to the client.
 */
public class RPCMessageIntegrationTest extends BaseIntegrationTest {
  private static final long SESSION_ID = 10;
  private static final long BLOCK_ID = 11;
  private static final long OFFSET = 22;
  private static final long LENGTH = 33;
  private static final long LOCK_ID = 44;

  // This channel initializer sets up a simple pipeline with the encoder and decoder.
  private static class PipelineInitializer extends ChannelInitializer<SocketChannel> {
    private MessageSavingHandler mHandler = null;

    public PipelineInitializer(MessageSavingHandler handler) {
      mHandler = handler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast("frameDecoder", RPCMessage.createFrameDecoder());
      pipeline.addLast("RPCMessageDecoder", new RPCMessageDecoder());
      pipeline.addLast("RPCMessageEncoder", new RPCMessageEncoder());
      pipeline.addLast("handler", mHandler);
    }
  }

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private Channel mOutgoingChannel;

  private static NioEventLoopGroup sEventClient;
  private static NioEventLoopGroup sEventServer;
  private static MessageSavingHandler sIncomingHandler;
  private static Bootstrap sBootstrapClient;
  private static SocketAddress sLocalAddress;

  @BeforeClass
  public static void beforeClass() {
    sEventClient = new NioEventLoopGroup(1);
    sEventServer = new NioEventLoopGroup(1);
    sIncomingHandler = new MessageSavingHandler();

    // Setup the server.
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(sEventServer);
    bootstrap.channel(NioServerSocketChannel.class);
    bootstrap.childHandler(new PipelineInitializer(sIncomingHandler));

    InetSocketAddress address =
        new InetSocketAddress(NetworkAddressUtils.getLocalHostName(100),
            Integer.parseInt(PropertyKey.MASTER_RPC_PORT.getDefaultValue()));
    ChannelFuture cf = bootstrap.bind(address).syncUninterruptibly();
    sLocalAddress = cf.channel().localAddress();

    // Setup the client.
    sBootstrapClient = new Bootstrap();
    sBootstrapClient.group(sEventClient);
    sBootstrapClient.channel(NioSocketChannel.class);
    sBootstrapClient.handler(new PipelineInitializer(new MessageSavingHandler()));
  }

  @AfterClass
  public static void afterClass() {
    // Shut everything down.
    sEventClient.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    sEventServer.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    sEventClient.terminationFuture().syncUninterruptibly();
    sEventServer.terminationFuture().syncUninterruptibly();
  }

  @Before
  public final void before() {
    sIncomingHandler.reset();

    // Connect to the server.
    ChannelFuture cf = sBootstrapClient.connect(sLocalAddress).syncUninterruptibly();
    mOutgoingChannel = cf.channel();
  }

  @After
  public final void after() {
    // Close the client connection.
    mOutgoingChannel.close().syncUninterruptibly();
  }

  private void assertValid(RPCSaslTokenRequest expected, RPCSaslTokenRequest actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getEncodedLength(), actual.getEncodedLength());
    Assert.assertEquals(expected.getLength(), actual.getLength());
    if (expected.getLength() == 0) {
      // Length is 0, so payloads should be null.
      Assert.assertNull(expected.getPayloadDataBuffer());
      Assert.assertNull(actual.getPayloadDataBuffer());
    } else {
      Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer((int) OFFSET, (int) LENGTH, actual
          .getPayloadDataBuffer().getReadOnlyByteBuffer()));
    }
  }

  private void assertValid(RPCSaslCompleteResponse expected, RPCSaslCompleteResponse actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getEncodedLength(), actual.getEncodedLength());
    Assert.assertEquals(expected.getStatus(), actual.getStatus());
  }

  /**
   * Encodes and decodes the 'msg' by sending it through the client and server pipelines.
   */
  private RPCMessage encodeThenDecode(RPCMessage msg) {
    // Write the message to the outgoing channel.
    mOutgoingChannel.writeAndFlush(msg);
    // Read the decoded message from the incoming side.
    return sIncomingHandler.getMessage();
  }

  @Test
  public void RPCSaslTokenRequest() {
    ByteBuffer payload = BufferUtils.getIncreasingByteBuffer((int) OFFSET, (int) LENGTH);
    RPCSaslTokenRequest msg = new RPCSaslTokenRequest(new DataByteBuffer(payload, LENGTH));
    RPCSaslTokenRequest decoded = (RPCSaslTokenRequest) encodeThenDecode(msg);
    assertValid(msg, decoded);
  }

  @Test
  public void RPCSaslCompleteResponse() {
    RPCSaslCompleteResponse msg = new RPCSaslCompleteResponse(RPCResponse.Status.SUCCESS);
    RPCSaslCompleteResponse decoded = (RPCSaslCompleteResponse) encodeThenDecode(msg);
    assertValid(msg, decoded);
  }
}
