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

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.client.EncryptionMetaFactory;
import alluxio.client.LayoutUtils;
import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.block.stream.TestBlockInStream;
import alluxio.client.block.stream.TestBlockOutStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.security.CryptoUtils;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.PreconditionMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.security.EncryptionProto;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests for the {@link CryptoFileInStream} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, AlluxioBlockStore.class})
@PowerMockIgnore("javax.crypto.*")
public final class CryptoFileInStreamTest {

  private AlluxioBlockStore mBlockStore;
  private FileSystemContext mContext;
  private FileInfo mInfo;
  private URIStatus mStatus;

  private List<TestBlockOutStream> mCacheStreams;

  private long mLogicalBlockLength;
  private long mLogicalFileLength;
  private long mPhysicalBlockLength;
  private long mPhysicalFileLength;
  private long mFileFooterLength;
  private long mNumStreams;

  private EncryptionProto.Meta mMeta;
  private CryptoFileInStream mTestStream;

  private long getBlockLength(int streamId) {
    return streamId == mNumStreams - 1 ? mFileFooterLength : mPhysicalBlockLength;
  }

  private byte[] getBlockData(int streamId) {
    // The file has a first full block, a second block with fullBlockSize-1
    // The file footer is split across two physical chunks.
    byte[] fileFooter = LayoutUtils.encodeFooter(mMeta);
    if (streamId == mNumStreams - 1) {
      return Arrays.copyOfRange(fileFooter, 1, fileFooter.length);
    } else if (streamId == mNumStreams - 2) {
      byte[] combined = new byte[(int) mPhysicalBlockLength];
      byte[] plaintext = BufferUtils.getIncreasingByteArray(
          (int) (streamId * mLogicalBlockLength), (int) mLogicalBlockLength - 1);
      ByteBuf ciphertext = CryptoUtils.encryptChunks(mMeta, Unpooled.wrappedBuffer(plaintext));
      ciphertext.readBytes(combined, 0, ciphertext.readableBytes());
      combined[(int) mPhysicalBlockLength - 1] = fileFooter[fileFooter.length - 1];
      return combined;
    } else {
      byte[] plaintext = BufferUtils.getIncreasingByteArray(
          (int) (streamId * mLogicalBlockLength), (int) mLogicalBlockLength);
      ByteBuf ciphertext = CryptoUtils.encryptChunks(mMeta, Unpooled.wrappedBuffer(plaintext));
      return ciphertext.array();
    }
  }

  /**
   * Sets up the context and streams before a test runs.
   */
  @Before
  public void before() throws Exception {
    // Set logical block size to be 4 full chunks size.
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "256KB");
    mMeta = EncryptionMetaFactory.create();
    // One full blocks, one partial block + 1st part of footer, and the 2nd part of footer in the
    // last physical block.
    mNumStreams = 3;
    mLogicalBlockLength = mMeta.getLogicalBlockSize();
    mPhysicalBlockLength = mMeta.getPhysicalBlockSize();
    mFileFooterLength = mMeta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
    mLogicalFileLength = mMeta.getLogicalBlockSize() * (mNumStreams - 1) - 1;
    mPhysicalFileLength = mMeta.getPhysicalBlockSize() * (mNumStreams - 1) - 1 + mFileFooterLength;
    mInfo = new FileInfo().setBlockSizeBytes(mPhysicalBlockLength).setLength(mPhysicalFileLength);

    ClientTestUtils.setSmallBufferSizes();

    mContext = PowerMockito.mock(FileSystemContext.class);
    PowerMockito.when(mContext.getLocalWorker()).thenReturn(new WorkerNetAddress());
    mBlockStore = Mockito.mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mContext)).thenReturn(mBlockStore);
    PowerMockito.when(mBlockStore.getWorkerInfoList()).thenReturn(new ArrayList<BlockWorkerInfo>());

    // Set up BufferedBlockInStreams and caching streams
    mCacheStreams = new ArrayList<>();
    List<Long> blockIds = new ArrayList<>();
    for (int i = 0; i < mNumStreams; i++) {
      blockIds.add((long) i);
      mCacheStreams.add(new TestBlockOutStream(
          ByteBuffer.allocate((int) mPhysicalBlockLength), getBlockLength(i)));
      Mockito.when(mBlockStore.getWorkerInfoList())
          .thenReturn(Arrays.asList(new BlockWorkerInfo(new WorkerNetAddress(), 0, 0)));
      Mockito
          .when(mBlockStore.getInStream(Mockito.eq((long) i), Mockito.any(
              Protocol.OpenUfsBlockOptions.class), Mockito.any(InStreamOptions.class)))
          .thenAnswer(new Answer<BlockInStream>() {
            @Override
            public BlockInStream answer(InvocationOnMock invocation) throws Throwable {
              long i = (Long) invocation.getArguments()[0];
              byte[] input = getBlockData((int) i);
              return new TestBlockInStream(input, i, input.length, false);
            }
          });
      Mockito.when(mBlockStore.getOutStream(Mockito.eq((long) i), Mockito.anyLong(),
          Mockito.any(WorkerNetAddress.class), Mockito.any(OutStreamOptions.class)))
          .thenAnswer(new Answer<BlockOutStream>() {
            @Override
            public BlockOutStream answer(InvocationOnMock invocation) throws Throwable {
              long i = (Long) invocation.getArguments()[0];
              return mCacheStreams.get((int) i).isClosed() ? null : mCacheStreams.get((int) i);
            }
          });
    }
    mInfo.setBlockIds(blockIds);
    mStatus = new URIStatus(mInfo);

    InStreamOptions options = InStreamOptions.defaults()
        .setReadType(ReadType.CACHE_PROMOTE)
        .setCachePartiallyReadBlock(false)
        .setEncrypted(true)
        .setEncryptionMeta(mMeta);
    mTestStream = new CryptoFileInStream(mStatus, options, mContext);
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
    ClientTestUtils.resetClient();
  }

  @Test
  public void singleByteRead() throws Exception {
    for (int i = 0; i < mLogicalFileLength; i++) {
      Assert.assertEquals(i & 0xff, mTestStream.read());
    }
    mTestStream.close();
  }

  @Test
  public void readHalfFile() throws Exception {
    testReadBuffer((int) (mLogicalFileLength / 2));
  }

  @Test
  public void readPartialBlock() throws Exception {
    testReadBuffer((int) (mLogicalBlockLength / 2));
  }

  @Test
  public void readBlock() throws Exception {
    testReadBuffer((int) mLogicalBlockLength);
  }

  @Test
  public void readFile() throws Exception {
    testReadBuffer((int) mLogicalFileLength);
  }

  @Test
  public void readOffset() throws IOException {
    int offset = (int) (mLogicalBlockLength / 3);
    int len = (int) mLogicalBlockLength;
    byte[] buffer = new byte[offset + len];
    // Create expectedBuffer containing `offset` 0's followed by `len` increasing bytes
    byte[] expectedBuffer = new byte[offset + len];
    System.arraycopy(BufferUtils.getIncreasingByteArray(len), 0, expectedBuffer, offset, len);
    mTestStream.read(buffer, offset, len);
    Assert.assertArrayEquals(expectedBuffer, buffer);
    mTestStream.close();
  }

  @Test
  public void remaining() throws IOException {
    Assert.assertEquals(mLogicalFileLength, mTestStream.remaining());
    mTestStream.read();
    Assert.assertEquals(mLogicalFileLength - 1, mTestStream.remaining());
    mTestStream.read(new byte[150]);
    Assert.assertEquals(mLogicalFileLength - 151, mTestStream.remaining());
    mTestStream.skip(140);
    Assert.assertEquals(mLogicalFileLength - 291, mTestStream.remaining());
    mTestStream.seek(310);
    Assert.assertEquals(mLogicalFileLength - 310, mTestStream.remaining());
    mTestStream.seek(130);
    Assert.assertEquals(mLogicalFileLength - 130, mTestStream.remaining());
    mTestStream.close();
  }

  @Test
  public void seek() throws IOException {
    int seekAmount = (int) (mLogicalBlockLength / 2);
    int readAmount = (int) (mLogicalBlockLength);
    byte[] buffer = new byte[readAmount];
    // Seek halfway into block 1
    mTestStream.seek(seekAmount);
    // Read two blocks from 0.5 to 1.5
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(seekAmount, readAmount), buffer);
    mTestStream.close();
  }

  @Test
  public void seekToEOF() throws IOException {
    mTestStream.seek((int) (mLogicalFileLength));
    Assert.assertEquals(-1, mTestStream.read());
    mTestStream.close();
  }

  @Test
  public void longSeekBackward() throws IOException {
    int seekAmount = (int) (mLogicalBlockLength / 4 + mLogicalBlockLength);
    int readAmount = (int) (mLogicalBlockLength * 2 - mLogicalBlockLength / 2);
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount - seekAmount);

    byte[] actual = new byte[(int) mLogicalBlockLength / 2];
    mTestStream.read(actual);
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray(
            (int) mLogicalBlockLength / 4, (int) mLogicalBlockLength / 2),
        actual);
    mTestStream.close();
  }

  @Test
  public void shortSeekBackward() throws IOException {
    int seekAmount = 5;
    int readAmount = (int) mLogicalBlockLength / 2;
    byte[] buffer = new byte[readAmount];
    mTestStream.read(buffer);

    // Seek backward.
    mTestStream.seek(readAmount - seekAmount);

    byte[] actual = new byte[(int) mLogicalBlockLength];
    mTestStream.read(actual);
    Assert.assertArrayEquals(
        BufferUtils.getIncreasingByteArray(
            (int) mLogicalBlockLength / 2 - 5, (int) mLogicalBlockLength),
        actual);
    mTestStream.close();
  }

  @Test
  public void skip() throws IOException {
    int skipAmount = (int) (mLogicalBlockLength / 4);
    int readAmount = (int) (mLogicalBlockLength);
    byte[] buffer = new byte[readAmount];
    // Skip halfway into block 1
    mTestStream.skip(skipAmount);
    // Read two blocks from 0.25 to 1.25
    mTestStream.read(buffer);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(skipAmount, readAmount), buffer);

    Assert.assertEquals(0, mTestStream.skip(0));
    // Skip to 1.75
    Assert.assertEquals(skipAmount * 2, mTestStream.skip(skipAmount * 2));
    // Read one byte
    Assert.assertEquals((skipAmount + readAmount + skipAmount * 2) & 0xff, mTestStream.read());
    // Skip to EOF
    Assert.assertEquals(skipAmount - 2, mTestStream.skip(skipAmount - 2));
    Assert.assertEquals(-1, mTestStream.read());
    mTestStream.close();
  }

  @Test
  public void readOutOfBounds() throws IOException {
    mTestStream.read(new byte[(int) mLogicalFileLength]);
    Assert.assertEquals(-1, mTestStream.read());
    Assert.assertEquals(-1, mTestStream.read(new byte[10]));
  }

  @Test
  public void readBadBuffer() throws IOException {
    try {
      mTestStream.read(new byte[10], 5, 6);
      Assert.fail("the buffer read of invalid offset/length should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_BUFFER_STATE.toString(), 10, 5, 6),
          e.getMessage());
    }
  }

  @Test
  public void seekNegative() throws IOException {
    try {
      mTestStream.seek(-1);
      Assert.fail("seeking negative position should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), -1),
          e.getMessage());
    }
  }

  @Test
  public void seekPastEnd() throws IOException {
    try {
      mTestStream.seek(mLogicalFileLength + 1);
      Assert.fail("seeking past the end of the stream should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(String.format(PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(),
          mLogicalFileLength + 1), e.getMessage());
    }
  }

  @Test
  public void skipNegative() throws IOException {
    Assert.assertEquals(0, mTestStream.skip(-10));
  }

  @Test
  public void missingLocationPolicy() {
    try {
      mTestStream = new CryptoFileInStream(mStatus,
          InStreamOptions.defaults().setReadType(ReadType.CACHE).setLocationPolicy(null), mContext);
    } catch (NullPointerException e) {
      Assert.assertEquals(PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED.toString(),
          e.getMessage());
    }
  }

  @Test
  public void positionedRead() throws Exception {
    class TestCase {
      long mPos;
      int mLen;

      public TestCase(long pos, int len) {
        mPos = pos;
        mLen = len;
      }
    }

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, 1));
    testCases.add(new TestCase(0, (int) mLogicalFileLength));
    testCases.add(new TestCase(1, 1));
    testCases.add(new TestCase(1, (int) mMeta.getChunkSize() + 1));
    testCases.add(new TestCase(1, (int) mLogicalBlockLength + 2));
    testCases.add(new TestCase(mMeta.getChunkSize() - 1, 1));
    testCases.add(new TestCase(mMeta.getChunkSize() - 1, 2));
    testCases.add(new TestCase(mMeta.getChunkSize() - 1, (int) mMeta.getChunkSize()));
    testCases.add(new TestCase(mMeta.getChunkSize(), 1));
    testCases.add(new TestCase(mLogicalBlockLength - 1, 1));
    testCases.add(new TestCase(mLogicalBlockLength + 1, (int) mLogicalBlockLength - 2));
    testCases.add(new TestCase(mLogicalBlockLength + 2, (int) mLogicalBlockLength - 4));
    testCases.add(new TestCase(mLogicalFileLength - 1, 1));
    for (TestCase test : testCases) {
      byte[] buffer = new byte[test.mLen];
      long pos = test.mPos;
      mTestStream.positionedRead(pos, buffer, 0, test.mLen);
      Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray((int) pos, test.mLen), buffer);
      Assert.assertEquals(mLogicalFileLength, mTestStream.remaining());
    }
    mTestStream.close();
  }

  @Test
  public void positionedReadReturnsLessThanRequested() throws Exception {
    // seek at the EOF does not affect positionedRead
    int seekPos = (int) mLogicalFileLength;
    mTestStream.seek(seekPos);
    int len = 2;
    byte[] buffer = new byte[len];
    long pos = mLogicalFileLength - 1;
    // Got the data length from the pos to logical EOF
    Assert.assertEquals(mLogicalFileLength - pos, mTestStream.positionedRead(pos, buffer, 0, len));
    Assert.assertEquals(mLogicalFileLength - seekPos, mTestStream.remaining());
    mTestStream.close();
  }

  @Test
  public void positionedReadFailed() throws Exception {
    byte[] buffer = new byte[(int) mLogicalBlockLength];
    Assert.assertEquals(-1, mTestStream.positionedRead(mLogicalFileLength, buffer, 0, 1));
    Assert.assertEquals(-1, mTestStream.positionedRead(-1, buffer, 0, 1));
    mTestStream.close();
  }

  private void testReadBuffer(int dataRead) throws Exception {
    byte[] buffer = new byte[dataRead];
    mTestStream.read(buffer);
    mTestStream.close();

    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(dataRead), buffer);
  }
}
