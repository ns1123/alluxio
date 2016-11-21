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

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.netty.ClientHandler;
import alluxio.client.netty.NettyClient;
import alluxio.client.netty.NettySecureRpcClient;
import alluxio.client.netty.SingleResponseListener;
import alluxio.network.protocol.RPCRequest;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.RPCSecretKeyWriteRequest;
import alluxio.util.CommonUtils;
import alluxio.worker.AlluxioWorkerService;

import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Unit tests for {@link NettySecureRpcServer}.
 */
public final class NettySecureRpcServerTest {
  private NettySecureRpcServer mNettySecureRpcServer;

  @Rule
  public ConfigurationRule mRule = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, "0"));

  @Before
  public void before() throws Exception {
    AlluxioWorkerService alluxioWorker = Mockito.mock(AlluxioWorkerService.class);
    mNettySecureRpcServer = new NettySecureRpcServer(new InetSocketAddress(0), alluxioWorker);
  }

  @After
  public void after() throws Exception {
    mNettySecureRpcServer.close();
  }

  @Test
  public void close() throws Exception {
    mNettySecureRpcServer.close();
  }

  @Test
  public void port() {
    Assert.assertTrue(mNettySecureRpcServer.getPort() > 0);
  }

  @Test
  public void writeCapabilityKey() throws Exception {
    long keyId = 1L;
    long expirationTimeMs = CommonUtils.getCurrentMs() + 10 * 1000;
    byte[] encodedKey = "testkey".getBytes();
    RPCResponse response =
        request(new RPCSecretKeyWriteRequest(keyId, expirationTimeMs, encodedKey));
    Assert.assertEquals(RPCResponse.Status.SUCCESS, response.getStatus());
  }

  @Test
  public void writeWithNonSslClient() throws Exception {
    long keyId = 1L;
    long expirationTimeMs = CommonUtils.getCurrentMs() + 10 * 1000;
    byte[] encodedKey = "testkey".getBytes();

    try {
      requestWithNonSslClient(
          new RPCSecretKeyWriteRequest(keyId, expirationTimeMs, encodedKey));
      Assert.fail("The server should get failure on non-ssl request and client should time out.");
    } catch (TimeoutException e) {
      // expected
    }
  }

  private RPCResponse request(RPCRequest rpcSecretKeyWriteRequest) throws Exception {
    InetSocketAddress address =
        new InetSocketAddress(mNettySecureRpcServer.getBindHost(),
            mNettySecureRpcServer.getPort());
    Bootstrap clientBootstrap = NettySecureRpcClient.createClientBootstrap(address);
    ChannelFuture f = clientBootstrap.connect(address).sync();
    Channel channel = f.channel();
    try {
      SingleResponseListener listener = new SingleResponseListener();
      ((ClientHandler) channel.pipeline().addLast(new ClientHandler()).last())
          .addListener(listener);
      channel.writeAndFlush(rpcSecretKeyWriteRequest);
      return listener.get(NettySecureRpcClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } finally {
      channel.close().sync();
    }
  }

  private RPCResponse requestWithNonSslClient(RPCRequest rpcSecretKeyWriteRequest)
      throws Exception {
    InetSocketAddress address =
        new InetSocketAddress(mNettySecureRpcServer.getBindHost(),
            mNettySecureRpcServer.getPort());
    Bootstrap clientBootstrap = NettyClient.createClientBootstrap();
    ChannelFuture f = clientBootstrap.connect(address).sync();
    Channel channel = f.channel();
    try {
      SingleResponseListener listener = new SingleResponseListener();
      ((ClientHandler) channel.pipeline().addLast(new ClientHandler()).last())
          .addListener(listener);
      channel.writeAndFlush(rpcSecretKeyWriteRequest);
      return listener.get(500 /* timeout in ms */, TimeUnit.MILLISECONDS);
    } finally {
      channel.close().sync();
    }
  }
}
