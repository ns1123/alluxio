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

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.TieredBlockStore;

import com.google.common.base.Suppliers;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;

public class UfsBlockWriteHandlerTest extends WriteHandlerTest {
  private OutputStream mOutputStream;
  private BlockWorker mBlockWorker;
  private BlockStore mBlockStore;

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new HashMap<PropertyKey, String>() {
        {
          put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
              AlluxioTestDirectory.createTemporaryDirectory("UfsBlockWriteHandlerTest-RootUfs")
                  .getAbsolutePath());
          put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, AlluxioTestDirectory
              .createTemporaryDirectory("UfsBlockWriteHandlerTest-WorkerDataFolder")
              .getAbsolutePath());
          put(PropertyKey.WORKER_TIERED_STORE_LEVELS, "1");
        }
      });

  @Before
  public void before() throws Exception {
    mFile = mTestFolder.newFile().getPath();
    mOutputStream = new FileOutputStream(mFile);
    mBlockStore = new TieredBlockStore();
    mBlockWorker = Mockito.mock(BlockWorker.class);

    Mockito.when(mBlockWorker.getBlockStore()).thenReturn(mBlockStore);
    UnderFileSystem mockUfs = Mockito.mock(UnderFileSystem.class);
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    UfsManager.UfsInfo ufsInfo =
        new UfsManager.UfsInfo(Suppliers.ofInstance(mockUfs), AlluxioURI.EMPTY_URI);
    Mockito.when(ufsManager.get(Mockito.anyLong())).thenReturn(ufsInfo);
    Mockito.when(mockUfs.create(Mockito.anyString(), Mockito.any(CreateOptions.class)))
        .thenReturn(mOutputStream);

    mChannel = new EmbeddedChannel(
        new UfsBlockWriteHandler(NettyExecutors.FILE_WRITER_EXECUTOR, mBlockWorker, ufsManager));
  }

  @After
  public void after() throws Exception {
    mOutputStream.close();
  }

  @Test
  public void noTempBlockFound() throws Exception {
    mChannel.writeInbound(newWriteRequest(0, newDataBuffer(PACKET_SIZE)));
    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(Status.PStatus.NOT_FOUND, writeResponse);
  }

  @Override
  protected Protocol.RequestType getWriteRequestType() {
    return Protocol.RequestType.UFS_BLOCK;
  }
}