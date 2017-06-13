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

package alluxio.client.file;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.LoginUserRule;
import alluxio.PropertyKey;
import alluxio.client.LayoutUtils;
import alluxio.client.WriteType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.TestBlockOutStream;
import alluxio.client.block.stream.UnderFileSystemFileOutStream;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.util.ClientTestUtils;
import alluxio.client.util.EncryptionMetaTestUtils;
import alluxio.exception.PreconditionMessage;
import alluxio.proto.security.EncryptionProto;
import alluxio.resource.DummyCloseableResource;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for the {@link CryptoFileOutStream} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class, AlluxioBlockStore.class,
    UnderFileSystemFileOutStream.class})
@PowerMockIgnore("javax.crypto.*")
public final class CryptoFileOutStreamTest {
  @Rule
  public LoginUserRule mLoginUser = new LoginUserRule("Test");

  private static final AlluxioURI FILE_NAME = new AlluxioURI("/encrypted_file");

  private FileSystemContext mFileSystemContext;
  private AlluxioBlockStore mBlockStore;
  private FileSystemMasterClient mFileSystemMasterClient;

  private Map<Long, TestBlockOutStream> mAlluxioOutStreamMap;
  private UnderFileSystemFileOutStream mUnderStorageOutputStream;

  private CryptoFileOutStream mTestStream;

  private EncryptionProto.Meta mMeta;
  private long mBlockLength;

  /**
   * Sets up the different contexts and clients before a test runs.
   */
  @Before
  public void before() throws Exception {
    // Set logical block size to be 4 full chunks size.
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "256KB");
    GroupMappingServiceTestUtils.resetCache();
    ClientTestUtils.setSmallBufferSizes();

    mMeta = EncryptionMetaTestUtils.create();
    mBlockLength = mMeta.getPhysicalBlockSize();
    // PowerMock enums and final classes
    mFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);

    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mFileSystemContext)).thenReturn(mBlockStore);

    when(mFileSystemContext.acquireMasterClientResource())
        .thenReturn(new DummyCloseableResource<>(mFileSystemMasterClient));
    when(mFileSystemMasterClient.getStatus(any(AlluxioURI.class), any(GetStatusOptions.class)))
        .thenReturn(new URIStatus(new FileInfo()));

    // Return sequentially increasing numbers for new block ids
    when(mFileSystemMasterClient.getNewBlockIdForFile(FILE_NAME))
        .thenAnswer(new Answer<Long>() {
          private long mCount = 0;

          @Override
          public Long answer(InvocationOnMock invocation) throws Throwable {
            return mCount++;
          }
        });

    // Set up out streams. When they are created, add them to outStreamMap
    final Map<Long, TestBlockOutStream> outStreamMap = new HashMap<>();
    when(mBlockStore.getOutStream(anyLong(), eq(mBlockLength),
        any(OutStreamOptions.class))).thenAnswer(new Answer<TestBlockOutStream>() {
          @Override
          public TestBlockOutStream answer(InvocationOnMock invocation) throws Throwable {
            Long blockId = invocation.getArgumentAt(0, Long.class);
            if (!outStreamMap.containsKey(blockId)) {
              TestBlockOutStream newStream = new TestBlockOutStream(
                  ByteBuffer.allocate((int) mBlockLength), mBlockLength);
              outStreamMap.put(blockId, newStream);
            }
            return outStreamMap.get(blockId);
          }
        });
    BlockWorkerInfo workerInfo =
        new BlockWorkerInfo(new WorkerNetAddress().setHost("localhost").setRpcPort(1)
            .setDataPort(2).setWebPort(3), Constants.GB, 0);
    when(mBlockStore.getWorkerInfoList()).thenReturn(Lists.newArrayList(workerInfo));
    mAlluxioOutStreamMap = outStreamMap;

    mUnderStorageOutputStream = PowerMockito.mock(UnderFileSystemFileOutStream.class);

    PowerMockito.mockStatic(UnderFileSystemFileOutStream.class);
    PowerMockito.when(
        UnderFileSystemFileOutStream.create(any(FileSystemContext.class),
            any(WorkerNetAddress.class), any(OutStreamOptions.class))).thenReturn(
        mUnderStorageOutputStream);

    OutStreamOptions options = OutStreamOptions.defaults()
        .setBlockSizeBytes(mMeta.getPhysicalBlockSize())
        .setWriteType(WriteType.CACHE_THROUGH).setUfsPath(FILE_NAME.getPath())
        .setEncrypted(true).setEncryptionMeta(mMeta);
    mTestStream = createTestStream(FILE_NAME, options);
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
    ClientTestUtils.resetClient();
  }

  @Test
  public void singleByteWrite() throws Exception {
    mTestStream.write(5);
    mTestStream.close();
    Assert.assertEquals(LayoutUtils.toPhysicalFileLength(mMeta, 1),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
  }

  @Test
  public void fullChunkWrite() throws Exception {
    byte[] fullChunk =
        new String(new char[(int) mMeta.getChunkSize()]).replace('\0', 'a').getBytes();
    mTestStream.write(fullChunk);
    mTestStream.close();
    Assert.assertEquals(LayoutUtils.toPhysicalFileLength(mMeta, fullChunk.length),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
  }

  @Test
  public void writeWithOffset() throws Exception {
    byte[] fullChunk =
        new String(new char[(int) mMeta.getChunkSize()]).replace('\0', 'a').getBytes();
    mTestStream.write(fullChunk, fullChunk.length / 2, fullChunk.length / 2);
    mTestStream.close();
    Assert.assertEquals(LayoutUtils.toPhysicalFileLength(mMeta, fullChunk.length / 2),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
  }

  @Test
  public void twoChunksWrite() throws Exception {
    byte[] twoChunks =
        new String(new char[(int) mMeta.getChunkSize() + 2]).replace('\0', 'a').getBytes();
    mTestStream.write(twoChunks);
    mTestStream.close();
    Assert.assertEquals(LayoutUtils.toPhysicalFileLength(mMeta, twoChunks.length),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
  }

  @Test
  public void fullBlockWrite() throws Exception {
    byte[] fullBlock =
        new String(new char[(int) mMeta.getLogicalBlockSize()]).replace('\0', 'a').getBytes();
    mTestStream.write(fullBlock);
    mTestStream.close();
    Assert.assertEquals(mMeta.getPhysicalBlockSize(),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
    long fileFooterSize = mMeta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
    Assert.assertEquals(fileFooterSize,
        mAlluxioOutStreamMap.get(1L).getWrittenData().length);
  }

  @Test
  public void footerSplitWrite() throws Exception {
    byte[] fullBlock =
        new String(new char[(int) mMeta.getLogicalBlockSize() - 2]).replace('\0', 'a').getBytes();
    mTestStream.write(fullBlock);
    mTestStream.close();
    Assert.assertEquals(mMeta.getPhysicalBlockSize(),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
    long fileFooterSize = mMeta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
    Assert.assertEquals(fileFooterSize - 2,
        mAlluxioOutStreamMap.get(1L).getWrittenData().length);
  }

  @Test
  public void flushPartialChunkAndClose() throws Exception {
    byte[] halfChunk =
        new String(new char[(int) mMeta.getChunkSize() / 2]).replace('\0', 'a').getBytes();
    mTestStream.write(halfChunk);
    mTestStream.flush();
    Assert.assertEquals(0, mAlluxioOutStreamMap.size());
    mTestStream.close();
    Assert.assertEquals(LayoutUtils.toPhysicalFileLength(mMeta, halfChunk.length),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
  }

  @Test
  public void flushPartialChunkAndWriteOneByte() throws Exception {
    byte[] halfChunk =
        new String(new char[(int) mMeta.getChunkSize() / 2]).replace('\0', 'a').getBytes();
    mTestStream.write(halfChunk);
    mTestStream.flush();
    Assert.assertEquals(0, mAlluxioOutStreamMap.size());
    mTestStream.write(1);
    mTestStream.flush();
    Assert.assertEquals(0, mAlluxioOutStreamMap.size());
    mTestStream.close();
    Assert.assertEquals(LayoutUtils.toPhysicalFileLength(mMeta, halfChunk.length + 1),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
  }

  @Test
  public void flushPartialChunkAndAppendBytes() throws Exception {
    byte[] halfChunk =
        new String(new char[(int) mMeta.getChunkSize() / 2]).replace('\0', 'a').getBytes();
    mTestStream.write(halfChunk);
    mTestStream.flush();
    Assert.assertEquals(0, mAlluxioOutStreamMap.size());
    mTestStream.write(halfChunk);
    mTestStream.flush();
    Assert.assertEquals(LayoutUtils.toPhysicalChunksLength(mMeta, halfChunk.length * 2),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
    mTestStream.close();
    Assert.assertEquals(LayoutUtils.toPhysicalFileLength(mMeta, halfChunk.length * 2),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
  }

  @Test
  public void flushFullChunksAndAppend() throws Exception {
    byte[] twoFullChunk =
        new String(new char[(int) mMeta.getChunkSize() * 2]).replace('\0', 'a').getBytes();
    mTestStream.write(twoFullChunk);
    mTestStream.flush();
    Assert.assertEquals(LayoutUtils.toPhysicalChunksLength(mMeta, twoFullChunk.length),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
    mTestStream.write(1);
    mTestStream.flush();
    Assert.assertEquals(LayoutUtils.toPhysicalChunksLength(mMeta, twoFullChunk.length),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
    mTestStream.close();
    Assert.assertEquals(LayoutUtils.toPhysicalFileLength(mMeta, twoFullChunk.length + 1),
        mAlluxioOutStreamMap.get(0L).getWrittenData().length);
  }

  @Test
  public void writeNull() throws Exception {
    try {
      mTestStream.write(null);
      Assert.fail("writing null should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(PreconditionMessage.ERR_WRITE_BUFFER_NULL.toString(), e.getMessage());
    }
  }

  /**
   * Creates a {@link CryptoFileOutStream} for test.
   *
   * @param path the file path
   * @param options the set of options specific to this operation
   * @return a {@link CryptoFileOutStream}
   */
  private CryptoFileOutStream createTestStream(AlluxioURI path, OutStreamOptions options)
      throws IOException {
    return new CryptoFileOutStream(path, options, mFileSystemContext);
  }
}
