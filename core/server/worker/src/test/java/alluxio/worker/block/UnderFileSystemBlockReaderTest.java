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

package alluxio.worker.block;

<<<<<<< HEAD
import alluxio.AlluxioURI;
||||||| merged common ancestors
=======
import alluxio.AlluxioTestDirectory;
>>>>>>> alluxio/branch-1.5
import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsManager.UfsInfo;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;

import com.google.common.base.Suppliers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.HashMap;

public final class UnderFileSystemBlockReaderTest {
  private static final long TEST_BLOCK_SIZE = 1024;
  private static final long SESSION_ID = 1;
  private static final long BLOCK_ID = 2;

  private UnderFileSystemBlockReader mReader;
  private BlockStore mAlluxioBlockStore;
  private UnderFileSystemBlockMeta mUnderFileSystemBlockMeta;
  private UfsManager mUfsManager;
  private Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new HashMap<PropertyKey, String>() {
        {
          put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, AlluxioTestDirectory
              .createTemporaryDirectory("UnderFileSystemBlockReaderTest-RootUfs")
              .getAbsolutePath());
          // ensure tiered storage uses different tmp dir for each test case
          put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, AlluxioTestDirectory
              .createTemporaryDirectory("UnderFileSystemBlockReaderTest-WorkerDataFolder")
              .getAbsolutePath());
          put(PropertyKey.WORKER_TIERED_STORE_LEVELS, "1");
        }
      });

  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mFolder.getRoot().getAbsolutePath());

    String testFilePath = mFolder.newFile().getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE * 2);
    BufferUtils.writeBufferToFile(testFilePath, buffer);

    mAlluxioBlockStore = new TieredBlockStore();
    mUfsManager = Mockito.mock(UfsManager.class);
<<<<<<< HEAD
    UfsInfo ufsInfo = new UfsInfo(
        Suppliers.ofInstance(UnderFileSystem.Factory.create(testFilePath)),
        new AlluxioURI(testFilePath));
    Mockito.when(mAlluxioBlockStore
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.any(BlockStoreLocation.class),
            Mockito.anyLong())).thenReturn(mTempBlockMeta);
    Mockito.when(mTempBlockMeta.getPath()).thenReturn(mFolder.newFile().getAbsolutePath());
    Mockito.when(mUfsManager.get(Mockito.anyLong())).thenReturn(ufsInfo);
||||||| merged common ancestors
    Mockito.when(mAlluxioBlockStore
        .createBlock(Mockito.anyLong(), Mockito.anyLong(), Mockito.any(BlockStoreLocation.class),
            Mockito.anyLong())).thenReturn(mTempBlockMeta);
    Mockito.when(mTempBlockMeta.getPath()).thenReturn(mFolder.newFile().getAbsolutePath());
    Mockito.when(mUfsManager.get(Mockito.anyLong()))
        .thenReturn(UnderFileSystem.Factory.create(testFilePath));
=======
    Mockito.when(mUfsManager.get(Mockito.anyLong()))
        .thenReturn(UnderFileSystem.Factory.create(testFilePath));
>>>>>>> alluxio/branch-1.5

    mOpenUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder().setMaxUfsReadConcurrency(10)
        .setBlockSize(TEST_BLOCK_SIZE).setOffsetInFile(TEST_BLOCK_SIZE).setUfsPath(testFilePath)
        .build();
    mUnderFileSystemBlockMeta =
        new UnderFileSystemBlockMeta(SESSION_ID, BLOCK_ID, mOpenUfsBlockOptions);
  }

  private void checkTempBlock(long start, long length) throws Exception {
    Assert.assertNotNull(mAlluxioBlockStore.getTempBlockMeta(SESSION_ID, BLOCK_ID));
    mAlluxioBlockStore.commitBlock(SESSION_ID, BLOCK_ID);
    long lockId = mAlluxioBlockStore.lockBlock(SESSION_ID, BLOCK_ID);
    BlockReader reader = mAlluxioBlockStore.getBlockReader(SESSION_ID, BLOCK_ID, lockId);
    Assert.assertEquals(length, reader.getLength());
    ByteBuffer buffer = reader.read(0, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer((int) start, (int) length, buffer));
  }

  @Test
  public void readFullBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
    mReader.close();
    checkTempBlock(0, TEST_BLOCK_SIZE);
  }

  @Test
  public void readPartialBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE - 1);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE - 1, buffer));
    mReader.close();
    // partial block should not be cached
    Assert.assertNull(mAlluxioBlockStore.getTempBlockMeta(SESSION_ID, BLOCK_ID));
  }

  @Test
  public void offset() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(2, TEST_BLOCK_SIZE - 2);
    Assert.assertTrue(BufferUtils
        .equalIncreasingByteBuffer(2, (int) TEST_BLOCK_SIZE - 2, buffer));
    mReader.close();
    // partial block should not be cached
    Assert.assertNull(mAlluxioBlockStore.getTempBlockMeta(SESSION_ID, BLOCK_ID));
  }

  @Test
  public void readOverlap() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 2, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(2, TEST_BLOCK_SIZE - 2);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(2, (int) TEST_BLOCK_SIZE - 2, buffer));
    buffer = mReader.read(0, TEST_BLOCK_SIZE - 2);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE - 2, buffer));
    buffer = mReader.read(3, TEST_BLOCK_SIZE);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(3, (int) TEST_BLOCK_SIZE - 3, buffer));
    mReader.close();
    // block should be cached as two reads covers the full block
    checkTempBlock(0, TEST_BLOCK_SIZE);
  }

  @Test
  public void readFullBlockNoCache() throws Exception {
    mUnderFileSystemBlockMeta = new UnderFileSystemBlockMeta(SESSION_ID, BLOCK_ID,
        mOpenUfsBlockOptions.toBuilder().setNoCache(true).build());
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);
    // read should succeed even if error is thrown when caching
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
    mReader.close();
    Assert.assertNull(mAlluxioBlockStore.getTempBlockMeta(SESSION_ID, BLOCK_ID));
  }

  @Test
  public void readFullBlockCacheError() throws Exception {
    mAlluxioBlockStore = Mockito.mock(BlockStore.class);
    Mockito.doThrow(new WorkerOutOfSpaceException("Ignored"))
        .when(mAlluxioBlockStore)
        .requestSpace(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuffer buffer = mReader.read(0, TEST_BLOCK_SIZE);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
    mReader.close();
    Assert.assertNull(mAlluxioBlockStore.getTempBlockMeta(SESSION_ID, BLOCK_ID));
  }

  @Test
  public void transferFullBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuf buf =
        PooledByteBufAllocator.DEFAULT.buffer((int) TEST_BLOCK_SIZE * 2, (int) TEST_BLOCK_SIZE * 2);
    try {
      while (buf.writableBytes() > 0 && mReader.transferTo(buf) != -1) {
      }
      Assert.assertTrue(BufferUtils
          .equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buf.nioBuffer()));
      mReader.close();
    } finally {
      buf.release();
    }
    checkTempBlock(0, TEST_BLOCK_SIZE);
  }

  @Test
  public void transferPartialBlock() throws Exception {
    mReader = UnderFileSystemBlockReader
        .create(mUnderFileSystemBlockMeta, 0, mAlluxioBlockStore, mUfsManager);
    ByteBuf buf =
        PooledByteBufAllocator.DEFAULT.buffer((int) TEST_BLOCK_SIZE / 2, (int) TEST_BLOCK_SIZE / 2);
    try {
      while (buf.writableBytes() > 0 && mReader.transferTo(buf) != -1) {
      }
      Assert.assertTrue(BufferUtils
          .equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE / 2, buf.nioBuffer()));
      mReader.close();
    } finally {
      buf.release();
    }
    // partial block should not be cached
    Assert.assertNull(mAlluxioBlockStore.getTempBlockMeta(SESSION_ID, BLOCK_ID));
  }
}
