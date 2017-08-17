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

import alluxio.AlluxioURI;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status.PStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsManager.UfsInfo;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.proto.ProtoMessage;

import com.google.common.base.Suppliers;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Unit tests for {@link UfsFileWriteHandler}.
 */
public final class UfsFileWriteHandlerTest extends WriteHandlerTest {
  private OutputStream mOutputStream;

  @Before
  public void before() throws Exception {
    mFile = mTestFolder.newFile().getPath();
    mOutputStream = new FileOutputStream(mFile);
    UnderFileSystem mockUfs = Mockito.mock(UnderFileSystem.class);
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    UfsInfo ufsInfo = new UfsInfo(Suppliers.ofInstance(mockUfs), AlluxioURI.EMPTY_URI);
    Mockito.when(ufsManager.get(Mockito.anyLong())).thenReturn(ufsInfo);
    Mockito.when(mockUfs.create(Mockito.anyString(), Mockito.any(CreateOptions.class)))
        .thenReturn(mOutputStream);

    mChannel = new EmbeddedChannel(
        new UfsFileWriteHandler(NettyExecutors.FILE_WRITER_EXECUTOR, ufsManager));
  }

  @After
  public void after() throws Exception {
    mOutputStream.close();
  }

  /**
   * Tests write failure.
   */
  @Test
  public void writeFailure() throws Exception {
    mChannel.writeInbound(newWriteRequest(0, newDataBuffer(PACKET_SIZE)));
    mOutputStream.close();
    mChannel.writeInbound(newWriteRequest(PACKET_SIZE, newDataBuffer(PACKET_SIZE)));
    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.UNKNOWN, writeResponse);
  }

  @Override
  protected RPCProtoMessage newWriteRequest(long offset, DataBuffer buffer) {
    Protocol.CreateUfsFileOptions createUfsFileOptions = Protocol.CreateUfsFileOptions.newBuilder()
        .setUfsPath("/test").setOwner("owner").setGroup("group").setMode(0).build();
    Protocol.WriteRequest writeRequest =
        Protocol.WriteRequest.newBuilder().setId(1L).setOffset(offset)
            .setCreateUfsFileOptions(createUfsFileOptions).setType(Protocol.RequestType.UFS_FILE)
            .build();
    return new RPCProtoMessage(new ProtoMessage(writeRequest), buffer);
  }

  @Override
  protected Protocol.RequestType getWriteRequestType() {
    return Protocol.RequestType.UFS_FILE;
  }
}
