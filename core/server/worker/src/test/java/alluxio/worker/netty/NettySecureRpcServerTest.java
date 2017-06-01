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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.netty.NettyClient;
import alluxio.client.netty.NettyRPC;
import alluxio.client.netty.NettyRPCContext;
import alluxio.client.netty.NettySecureRpcClient;
import alluxio.proto.security.Key;
import alluxio.security.capability.CapabilityKey;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.util.proto.ProtoUtils;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.security.CapabilityCache;

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

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Unit tests for {@link NettySecureRpcServer}.
 */
public final class NettySecureRpcServerTest {
  private NettySecureRpcServer mNettySecureRpcServer;
  private BlockWorker mBlockWorker;

  private CapabilityKey mKey;
  private CapabilityCache mCapabilityCache;

  @Rule
  public ConfigurationRule mRule = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, "0"));

  @Before
  public void before() throws Exception {
    mKey = new CapabilityKey(0L, CommonUtils.getCurrentMs() + Constants.DAY_MS,
        "1111111111111111111111111111111111111111111111111111".getBytes());
    mCapabilityCache = new CapabilityCache(
        CapabilityCache.Options.defaults().setCapabilityKey(mKey));
    mBlockWorker = Mockito.mock(BlockWorker.class);
    WorkerProcess workerProcess = Mockito.mock(WorkerProcess.class);
    Mockito.when(workerProcess.getWorker(BlockWorker.class)).thenReturn(mBlockWorker);
    Mockito.when(mBlockWorker.getCapabilityCache()).thenReturn(mCapabilityCache);
    mNettySecureRpcServer = new NettySecureRpcServer(new InetSocketAddress(0), workerProcess);
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

    Key.SecretKey request =
        ProtoUtils.setSecretKey(
            Key.SecretKey.newBuilder().setKeyType(Key.KeyType.CAPABILITY).setKeyId(keyId)
                .setExpirationTimeMs(expirationTimeMs), encodedKey).build();

    try {
      updateKey(request);
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void writeWithNonSslClient() throws Exception {
    long keyId = 1L;
    long expirationTimeMs = CommonUtils.getCurrentMs() + 10 * 1000;
    byte[] encodedKey = "testkey".getBytes();

    try {
      Key.SecretKey request =
          ProtoUtils.setSecretKey(
              Key.SecretKey.newBuilder().setKeyType(Key.KeyType.CAPABILITY).setKeyId(keyId)
                  .setExpirationTimeMs(expirationTimeMs), encodedKey).build();

      updateKeyNonSslClient(request);
      Assert.fail("The server should get failure on non-ssl request and client should time out.");
    } catch (IOException e) {
      // expected
    }
  }

  private void updateKey(Key.SecretKey key) throws Exception {
    InetSocketAddress address =
        new InetSocketAddress(mNettySecureRpcServer.getBindHost(),
            mNettySecureRpcServer.getPort());
    Bootstrap clientBootstrap = NettySecureRpcClient.createClientBootstrap(address);
    ChannelFuture f = clientBootstrap.connect(address).sync();
    Channel channel = f.channel();
    try {
      NettyRPC.call(NettyRPCContext.defaults().setTimeout(NettySecureRpcClient.TIMEOUT_MS)
          .setChannel(channel), new ProtoMessage(key));
    } finally {
      channel.close();
    }
  }

  private void updateKeyNonSslClient(Key.SecretKey key) throws Exception {
    InetSocketAddress address =
        new InetSocketAddress(mNettySecureRpcServer.getBindHost(),
            mNettySecureRpcServer.getPort());
    Bootstrap clientBootstrap = NettyClient.createClientBootstrap(address);
    ChannelFuture f = clientBootstrap.connect(address).sync();
    Channel channel = f.channel();
    try {
      NettyRPC.call(NettyRPCContext.defaults().setTimeout(500 /* timeout in ms */)
          .setChannel(channel), new ProtoMessage(key));
    } finally {
      channel.close();
    }
  }
}
