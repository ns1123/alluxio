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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.client.netty.ClientHandler;
import alluxio.client.netty.NettyClient;
import alluxio.client.netty.SingleResponseListener;
import alluxio.netty.NettyAttributes;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCFileReadRequest;
import alluxio.network.protocol.RPCFileWriteRequest;
import alluxio.network.protocol.RPCRequest;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataByteArrayChannel;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.AlluxioWorkerService;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.MockBlockReader;
import alluxio.worker.block.io.MockBlockWriter;
import alluxio.worker.file.FileSystemWorker;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Tests for NettyDataServer with Kerberos authentication enabled.
 */
public final class SaslNettyDataServerTest {
  private NettyDataServer mNettyDataServer;
  private BlockWorker mBlockWorker;
  private FileSystemWorker mFileSystemWorker;

  private static MiniKdc sKdc;
  private static File sWorkDir;
  private static String sHost;

  private static String sServerPrincipal;
  private static File sServerKeytab;

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @Rule
  public ConfigurationRule mRule = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, "0"));

  @BeforeClass
  public static void beforeClass() throws Exception {
    sWorkDir = FOLDER.getRoot();
    sKdc = new MiniKdc(MiniKdc.createConf(), sWorkDir);
    sKdc.start();

    sHost = NetworkAddressUtils.getLocalHostName();
    String realm = sKdc.getRealm();

    sServerPrincipal = "alluxio/" + sHost + "@" + realm;
    sServerKeytab = new File(sWorkDir, "alluxio.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sServerKeytab, "alluxio/" + sHost);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  @Before
  public void before() {
    LoginUserTestUtils.resetLoginUser();
    // Set server-side and client-side Kerberos configuration for Netty authentication.
    Configuration.set(PropertyKey.TEST_MODE, "true");
    Configuration.set(PropertyKey.MASTER_HOSTNAME, sHost);
    Configuration.set(PropertyKey.WORKER_HOSTNAME, sHost);
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio");

    mBlockWorker = Mockito.mock(BlockWorker.class);
    mFileSystemWorker = Mockito.mock(FileSystemWorker.class);
    AlluxioWorkerService alluxioWorker = Mockito.mock(AlluxioWorkerService.class);
    Mockito.when(alluxioWorker.getBlockWorker()).thenReturn(mBlockWorker);
    Mockito.when(alluxioWorker.getFileSystemWorker()).thenReturn(mFileSystemWorker);

    mNettyDataServer = new NettyDataServer(
        new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), 0), alluxioWorker);
  }

  @After
  public void after() throws Exception {
    mNettyDataServer.close();
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void readBlock() throws Exception {
    long sessionId = 0;
    long blockId = 1;
    long offset = 2;
    long length = 3;
    long lockId = 4;
    when(mBlockWorker.readBlockRemote(sessionId, blockId, lockId)).thenReturn(
        new MockBlockReader("abcdefg".getBytes(Charsets.UTF_8)));
    RPCResponse response =
        request(new RPCBlockReadRequest(blockId, offset, length, lockId, sessionId));

    // Verify that the 3 bytes were read at offset 2.
    assertEquals("cde",
        Charsets.UTF_8.decode(response.getPayloadDataBuffer().getReadOnlyByteBuffer()).toString());
  }

  @Test
  public void writeNewBlock() throws Exception {
    long sessionId = 0;
    long blockId = 1;
    long length = 2;
    // Offset is set to 0 so that a new block will be created.
    long offset = 0;
    DataByteArrayChannel data = new DataByteArrayChannel("abc".getBytes(Charsets.UTF_8), 0, 3);
    MockBlockWriter blockWriter = new MockBlockWriter();
    when(mBlockWorker.getTempBlockWriterRemote(sessionId, blockId)).thenReturn(blockWriter);
    RPCResponse response =
        request(new RPCBlockWriteRequest(sessionId, blockId, offset, length, data));

    // Verify that the write request tells the worker to create a new block and write the specified
    // data to it.
    assertEquals(RPCResponse.Status.SUCCESS, response.getStatus());
    verify(mBlockWorker).createBlockRemote(sessionId, blockId, "MEM", length);
    assertEquals("ab", new String(blockWriter.getBytes(), Charsets.UTF_8));
  }

  @Test
  public void writeExistingBlock() throws Exception {
    long sessionId = 0;
    long blockId = 1;
    // Offset is set to 1 so that the write is directed to an existing block
    long offset = 1;
    long length = 2;
    DataByteArrayChannel data = new DataByteArrayChannel("abc".getBytes(Charsets.UTF_8), 0, 3);
    MockBlockWriter blockWriter = new MockBlockWriter();
    when(mBlockWorker.getTempBlockWriterRemote(sessionId, blockId)).thenReturn(blockWriter);
    RPCResponse response =
        request(new RPCBlockWriteRequest(sessionId, blockId, offset, length, data));

    // Verify that the write request requests space on an existing block and then writes the
    // specified data.
    assertEquals(RPCResponse.Status.SUCCESS, response.getStatus());
    verify(mBlockWorker).requestSpace(sessionId, blockId, length);
    assertEquals("ab", new String(blockWriter.getBytes(), Charsets.UTF_8));
  }

  @Test
  public void readFile() throws Exception {
    long tempUfsFileId = 1;
    long offset = 0;
    long length = 3;
    when(mFileSystemWorker.getUfsInputStream(tempUfsFileId, offset)).thenReturn(
        new ByteArrayInputStream("abc".getBytes(Charsets.UTF_8)));
    RPCResponse response = request(new RPCFileReadRequest(tempUfsFileId, offset, length));

    // Verify that the 3 bytes were read.
    assertEquals("abc",
        Charsets.UTF_8.decode(response.getPayloadDataBuffer().getReadOnlyByteBuffer()).toString());
  }

  @Test
  public void writeFile() throws Exception {
    long tempUfsFileId = 1;
    long offset = 0;
    long length = 3;
    DataByteArrayChannel data = new DataByteArrayChannel("abc".getBytes(Charsets.UTF_8), 0, 3);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    when(mFileSystemWorker.getUfsOutputStream(tempUfsFileId)).thenReturn(outStream);
    RPCResponse response = request(new RPCFileWriteRequest(tempUfsFileId, offset, length, data));

    // Verify that the write request writes to the OutputStream returned by the worker.
    assertEquals(RPCResponse.Status.SUCCESS, response.getStatus());
    assertEquals("abc", new String(outStream.toByteArray(), Charsets.UTF_8));
  }

  /**
   * Creates a client bootstrap and waits until the channel is ready. Then send a RPCRequest to
   * the Netty server.
   */
  private RPCResponse request(RPCRequest request) throws Exception {
    InetSocketAddress address =
        new InetSocketAddress(mNettyDataServer.getBindHost(), mNettyDataServer.getPort());
    Bootstrap clientBootstrap = NettyClient.createClientBootstrap();
    clientBootstrap.attr(NettyAttributes.HOSTNAME_KEY, address.getHostName());
    ChannelFuture f = clientBootstrap.connect(address).sync();
    Channel channel = f.channel();
    // Waits for the channel authentication complete.
    NettyClient.waitForChannelReady(channel);
    try {
      SingleResponseListener listener = new SingleResponseListener();
      channel.pipeline().addLast(new ClientHandler()).get(ClientHandler.class)
          .addListener(listener);
      channel.writeAndFlush(request);
      return listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } finally {
      channel.close().sync();
    }
  }
}
