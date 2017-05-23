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

package alluxio.client;

import alluxio.Constants;
import alluxio.proto.layout.FileFooter;
import alluxio.proto.security.EncryptionProto;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Unit tests for {@link LayoutUtils}.
 */
public final class LayoutUtilsTest {
  private static final long BLOCK_HEADER_SIZE = 128L;
  private static final long BLOCK_FOOTER_SIZE = 64L;
  private static final long CHUNK_SIZE = Constants.DEFAULT_CHUNK_SIZE;
  private static final long CHUNK_HEADER_SIZE = 32L;
  private static final long CHUNK_FOOTER_SIZE = Constants.DEFAULT_CHUNK_FOOTER_SIZE;

  private final long mLogicalBlockSize = 64 * Constants.MB;
  private final long mPhysicalBlockSize = BLOCK_HEADER_SIZE + BLOCK_FOOTER_SIZE
      + mLogicalBlockSize / CHUNK_SIZE * (CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE);

  private static final String AES_GCM = Constants.AES_GCM_NOPADDING;
  private static final String TEST_SECRET_KEY = "yoursecretKey";
  private static final String TEST_IV = "ivvvv";
  private EncryptionProto.CryptoKey mKey =
      ProtoUtils.setIv(
          ProtoUtils.setKey(
              EncryptionProto.CryptoKey.newBuilder()
                  .setCipher(AES_GCM)
                  .setNeedsAuthTag(1)
                  .setGenerationId("generationBytes"), TEST_SECRET_KEY.getBytes()),
          TEST_IV.getBytes()).build();

  private FileFooter.FileMetadata mFileMetadata = FileFooter.FileMetadata.newBuilder()
      .setBlockHeaderSize(BLOCK_HEADER_SIZE)
      .setBlockFooterSize(BLOCK_FOOTER_SIZE)
      .setChunkHeaderSize(CHUNK_HEADER_SIZE)
      .setChunkSize(CHUNK_SIZE)
      .setChunkFooterSize(CHUNK_FOOTER_SIZE)
      .setEncryptionId(Constants.INVALID_ENCRYPTION_ID)
      .setPhysicalBlockSize(mPhysicalBlockSize)
      .build();

  private EncryptionProto.Meta mMeta = EncryptionProto.Meta.newBuilder()
      .setBlockHeaderSize(BLOCK_HEADER_SIZE)
      .setBlockFooterSize(BLOCK_FOOTER_SIZE)
      .setChunkHeaderSize(CHUNK_HEADER_SIZE)
      .setChunkSize(CHUNK_SIZE)
      .setChunkFooterSize(CHUNK_FOOTER_SIZE)
      .setFileId(Constants.INVALID_ENCRYPTION_ID)
      .setEncryptionId(Constants.INVALID_ENCRYPTION_ID)
      .setLogicalBlockSize(mLogicalBlockSize)
      .setPhysicalBlockSize(mPhysicalBlockSize)
      .setEncodedMetaSize(mFileMetadata.getSerializedSize())
      .setCryptoKey(mKey)
      .build();

  @Test
  public void toPhysicalOffset() throws Exception {
    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;
    final long[] logicalOffset = new long[]{
        1,
        10,
        CHUNK_SIZE,
        CHUNK_SIZE + 10,
        CHUNK_SIZE + CHUNK_SIZE - 1,
        CHUNK_SIZE + CHUNK_SIZE,
        CHUNK_SIZE + CHUNK_SIZE + CHUNK_SIZE - 10,
    };
    final long[] physicalChunkStart = new long[] {
        0,
        0,
        physicalChunkSize,
        physicalChunkSize,
        physicalChunkSize,
        physicalChunkSize + physicalChunkSize,
        physicalChunkSize + physicalChunkSize,
    };

    final long[] logicalChunkOffsetFromChunkStart = new long[] {
        1,
        10,
        0,
        10,
        CHUNK_SIZE - 1,
        0,
        CHUNK_SIZE - 10,
    };

    final long[] physicalChunkOffsetFromChunkStart = new long[] {
        CHUNK_HEADER_SIZE + 1,
        CHUNK_HEADER_SIZE + 10,
        CHUNK_HEADER_SIZE,
        CHUNK_HEADER_SIZE + 10,
        CHUNK_HEADER_SIZE + CHUNK_SIZE - 1,
        CHUNK_HEADER_SIZE,
        CHUNK_HEADER_SIZE + CHUNK_SIZE - 10,
    };

    final long[] physicalOffset = new long[] {
        CHUNK_HEADER_SIZE + 1,
        CHUNK_HEADER_SIZE + 10,
        physicalChunkSize + CHUNK_HEADER_SIZE,
        physicalChunkSize + CHUNK_HEADER_SIZE + 10,
        physicalChunkSize + CHUNK_HEADER_SIZE + CHUNK_SIZE - 1,
        physicalChunkSize + physicalChunkSize + CHUNK_HEADER_SIZE,
        physicalChunkSize + physicalChunkSize + CHUNK_HEADER_SIZE + CHUNK_SIZE - 10,
    };

    for (int i = 0; i < logicalOffset.length; i++) {
      Assert.assertEquals(physicalChunkStart[i],
          LayoutUtils.getPhysicalChunkStart(mMeta, logicalOffset[i]));
      Assert.assertEquals(logicalChunkOffsetFromChunkStart[i],
          LayoutUtils.getLogicalOffsetFromChunkStart(mMeta, logicalOffset[i]));
      Assert.assertEquals(physicalChunkOffsetFromChunkStart[i],
          LayoutUtils.getPhysicalOffsetFromChunkStart(mMeta, logicalOffset[i]));
      Assert.assertEquals(physicalOffset[i],
          LayoutUtils.toPhysicalOffset(mMeta, logicalOffset[i]));
    }
  }

  @Test
  public void toPhysicalChunksLength() throws Exception {
    class TestCase {
      long mExpected;
      long mLogicalLength;

      public TestCase(long expected, long logicalLength) {
        mExpected = expected;
        mLogicalLength = logicalLength;
      }
    }

    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, 0));
    testCases.add(new TestCase(CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE, 1));
    testCases.add(new TestCase(
        CHUNK_HEADER_SIZE + CHUNK_SIZE / 2 + CHUNK_FOOTER_SIZE, CHUNK_SIZE / 2));
    testCases.add(new TestCase(CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE, CHUNK_SIZE));
    testCases.add(new TestCase(
        physicalChunkSize + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE, CHUNK_SIZE + 1));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with logical length %d", testCase.mLogicalLength),
          testCase.mExpected, LayoutUtils.toPhysicalChunksLength(mMeta, testCase.mLogicalLength));
    }
  }

  @Test
  public void toPhysicalBlockLength() throws Exception {
    class TestCase {
      long mExpected;
      long mLogicalBlockLength;

      public TestCase(long expected, long logicalBlockLength) {
        mExpected = expected;
        mLogicalBlockLength = logicalBlockLength;
      }
    }

    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, 0));
    testCases.add(new TestCase(
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE + BLOCK_FOOTER_SIZE, 1));
    testCases.add(new TestCase(
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + CHUNK_SIZE / 2 + CHUNK_FOOTER_SIZE
            + BLOCK_FOOTER_SIZE,
        CHUNK_SIZE / 2));
    testCases.add(new TestCase(
        BLOCK_HEADER_SIZE + physicalChunkSize + BLOCK_FOOTER_SIZE, CHUNK_SIZE));
    testCases.add(new TestCase(
        BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE
            + BLOCK_FOOTER_SIZE,
        CHUNK_SIZE + 1));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with logical length %d", testCase.mLogicalBlockLength),
          testCase.mExpected,
          LayoutUtils.toPhysicalBlockLength(mMeta, testCase.mLogicalBlockLength));
    }
  }

  @Test
  public void toPhysicalFileLength() throws Exception {
    class TestCase {
      long mExpected;
      long mLogicalFileLength;

      public TestCase(long expected, long logicalFileLength) {
        mExpected = expected;
        mLogicalFileLength = logicalFileLength;
      }
    }

    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;
    final long footerSize = mMeta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, 0));
    testCases.add(new TestCase(
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE + BLOCK_FOOTER_SIZE
            + footerSize,
        1));
    testCases.add(new TestCase(
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + CHUNK_SIZE / 2 + CHUNK_FOOTER_SIZE
            + BLOCK_FOOTER_SIZE + footerSize,
        CHUNK_SIZE / 2));
    testCases.add(new TestCase(
        BLOCK_HEADER_SIZE + physicalChunkSize + BLOCK_FOOTER_SIZE + footerSize, CHUNK_SIZE));
    testCases.add(new TestCase(
        BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE
            + BLOCK_FOOTER_SIZE + footerSize,
        CHUNK_SIZE + 1));
    testCases.add(new TestCase(
        mMeta.getPhysicalBlockSize() + BLOCK_HEADER_SIZE + physicalChunkSize
            + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE + BLOCK_FOOTER_SIZE + footerSize,
        mMeta.getLogicalBlockSize() + CHUNK_SIZE + 1));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with logical length %d", testCase.mLogicalFileLength),
          testCase.mExpected,
          LayoutUtils.toPhysicalFileLength(mMeta, testCase.mLogicalFileLength));
    }
  }

  @Test
  public void toLogicalChunksLength() throws Exception {
    class TestCase {
      long mExpected;
      long mPhysicalLength;

      public TestCase(long expected, long physicalLength) {
        mExpected = expected;
        mPhysicalLength = physicalLength;
      }
    }

    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, 0));
    testCases.add(new TestCase(0, CHUNK_HEADER_SIZE + CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(1, CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(10, CHUNK_HEADER_SIZE + 10 + CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(
        physicalChunkSize / 2 - CHUNK_HEADER_SIZE - CHUNK_FOOTER_SIZE, physicalChunkSize / 2));
    testCases.add(new TestCase(CHUNK_SIZE, physicalChunkSize));
    testCases.add(new TestCase(
        CHUNK_SIZE + physicalChunkSize / 2 - CHUNK_HEADER_SIZE - CHUNK_FOOTER_SIZE,
        physicalChunkSize + physicalChunkSize / 2));
    testCases.add(new TestCase(2 * CHUNK_SIZE, 2 * physicalChunkSize));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with physical length %d", testCase.mPhysicalLength),
          testCase.mExpected, LayoutUtils.toLogicalChunksLength(mMeta, testCase.mPhysicalLength));
    }
  }

  @Test
  public void toLogicalBlockLength() throws Exception {
    class TestCase {
      long mExpected;
      long mPhysicalBlockLength;

      public TestCase(long expected, long physicalBlockLength) {
        mExpected = expected;
        mPhysicalBlockLength = physicalBlockLength;
      }
    }

    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, 0));
    testCases.add(new TestCase(0,
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + CHUNK_FOOTER_SIZE + BLOCK_FOOTER_SIZE));
    testCases.add(new TestCase(1,
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE + BLOCK_FOOTER_SIZE));
    testCases.add(new TestCase(10,
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 10 + CHUNK_FOOTER_SIZE + BLOCK_FOOTER_SIZE));
    testCases.add(new TestCase(
        physicalChunkSize / 2 - CHUNK_HEADER_SIZE - CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize / 2 + BLOCK_FOOTER_SIZE));
    testCases.add(new TestCase(CHUNK_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + BLOCK_FOOTER_SIZE));
    testCases.add(new TestCase(
        CHUNK_SIZE + physicalChunkSize / 2 - CHUNK_HEADER_SIZE - CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize / 2 + BLOCK_FOOTER_SIZE));
    testCases.add(new TestCase(2 * CHUNK_SIZE,
        BLOCK_HEADER_SIZE + 2 * physicalChunkSize + BLOCK_FOOTER_SIZE));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with physical length %d", testCase.mPhysicalBlockLength),
          testCase.mExpected,
          LayoutUtils.toLogicalBlockLength(mMeta, testCase.mPhysicalBlockLength));
    }
  }

  @Test
  public void toLogicalFileLength() throws Exception {
    class TestCase {
      long mExpected;
      long mPhysicalFileLength;

      public TestCase(long expected, long physicalFileLength) {
        mExpected = expected;
        mPhysicalFileLength = physicalFileLength;
      }
    }

    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;
    final long footerSize = mMeta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, 0));
    testCases.add(new TestCase(0,
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + CHUNK_FOOTER_SIZE + BLOCK_FOOTER_SIZE
            + footerSize));
    testCases.add(new TestCase(1,
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE + BLOCK_FOOTER_SIZE
            + footerSize));
    testCases.add(new TestCase(10,
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 10 + CHUNK_FOOTER_SIZE + BLOCK_FOOTER_SIZE
            + footerSize));
    testCases.add(new TestCase(
        physicalChunkSize / 2 - CHUNK_HEADER_SIZE - CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize / 2 + BLOCK_FOOTER_SIZE + footerSize));
    testCases.add(new TestCase(CHUNK_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + BLOCK_FOOTER_SIZE + footerSize));
    testCases.add(new TestCase(
        CHUNK_SIZE + physicalChunkSize / 2 - CHUNK_HEADER_SIZE - CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize / 2 + BLOCK_FOOTER_SIZE
            + footerSize));
    testCases.add(new TestCase(2 * CHUNK_SIZE,
        BLOCK_HEADER_SIZE + 2 * physicalChunkSize + BLOCK_FOOTER_SIZE + footerSize));
    testCases.add(new TestCase(mMeta.getLogicalBlockSize(),
        mMeta.getPhysicalBlockSize() + footerSize));
    testCases.add(new TestCase(mMeta.getLogicalBlockSize() - 1,
        mMeta.getPhysicalBlockSize() - 1 + footerSize));
    testCases.add(new TestCase(mMeta.getLogicalBlockSize() + 1,
        mMeta.getPhysicalBlockSize() + BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE
            + BLOCK_FOOTER_SIZE + footerSize));
    testCases.add(new TestCase(2 * mMeta.getLogicalBlockSize(),
        2 * mMeta.getPhysicalBlockSize() + footerSize));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with physical length %d", testCase.mPhysicalFileLength),
          testCase.mExpected,
          LayoutUtils.toLogicalFileLength(mMeta, testCase.mPhysicalFileLength));
    }
  }

  @Test
  public void getFooterMaxSize() throws Exception {
    Assert.assertEquals(mFileMetadata.getSerializedSize() + LayoutUtils.getFooterFixedOverhead(),
        LayoutUtils.getFooterMaxSize());
  }

  @Test
  public void convertEncryptionMetaAndFileMetadata() throws Exception {
    FileFooter.FileMetadata fileMetadata = LayoutUtils.fromEncryptionMeta(mMeta);
    EncryptionProto.Meta convertedMeta =
        LayoutUtils.fromFooterMetadata(mMeta.getFileId(), fileMetadata, mKey);
    Assert.assertEquals(mFileMetadata, fileMetadata);
    Assert.assertEquals(mMeta, convertedMeta);
  }

  @Test
  public void encodeAndDecodeFooter() throws Exception {
    byte[] encodedFooter = LayoutUtils.encodeFooter(mMeta);
    FileFooter.FileMetadata fileMetadata = LayoutUtils.decodeFooter(encodedFooter);
    Assert.assertEquals(mMeta,
        LayoutUtils.fromFooterMetadata(mMeta.getFileId(), fileMetadata, mKey));
  }

  @Test
  public void encodeAndDecodeWithFactoryCreatedMeta() throws Exception {
    EncryptionProto.Meta expected = EncryptionMetaFactory.create();
    byte[] encodedFooter = LayoutUtils.encodeFooter(expected);
    FileFooter.FileMetadata fileMetadata = LayoutUtils.decodeFooter(encodedFooter);
    Assert.assertEquals(expected,
        LayoutUtils.fromFooterMetadata(mMeta.getFileId(), fileMetadata, expected.getCryptoKey()));
  }

  @Test
  public void convertFileInfoToLogicalWithOneBlock() throws Exception {
    long footerSize = mMeta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
    long logicalFileLength = 4L;
    long physicalBlockSize =
        LayoutUtils.toPhysicalBlockLength(mMeta, logicalFileLength) + footerSize;
    long physicalFileSize = physicalBlockSize;
    List<Long> blockIds = new ArrayList<>();
    long blockId = 1L;
    blockIds.add(blockId);
    List<FileBlockInfo> fileBlockInfos = new ArrayList<>();
    fileBlockInfos.add(new FileBlockInfo().setBlockInfo(
        new BlockInfo().setBlockId(blockId).setLength(physicalBlockSize)));

    FileInfo fileInfo = new FileInfo()
        .setEncrypted(true)
        .setFileId(Constants.INVALID_ENCRYPTION_ID)
        .setLength(physicalFileSize)
        .setBlockSizeBytes(mPhysicalBlockSize)
        .setBlockIds(blockIds)
        .setFileBlockInfos(fileBlockInfos);

    FileInfo converted = LayoutUtils.convertFileInfoToLogical(fileInfo, mMeta);
    Assert.assertEquals(logicalFileLength, converted.getLength());
    Assert.assertEquals(mLogicalBlockSize, converted.getBlockSizeBytes());
    Assert.assertEquals(logicalFileLength,
        (converted.getFileBlockInfos().get(0)).getBlockInfo().getLength());
  }

  @Test
  public void convertFileInfoToLogicalWithTwoBlocks() throws Exception {
    long footerSize = mMeta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
    long logicalBlockSizeOne = mLogicalBlockSize;
    long logicalBlockSizeTwo = 4L;
    long logicalFileLength = logicalBlockSizeOne + logicalBlockSizeTwo;
    long physicalBlockOneSize = mPhysicalBlockSize;
    long physicalBlockTwoSize =
        LayoutUtils.toPhysicalBlockLength(mMeta, logicalBlockSizeTwo) + footerSize;
    long physicalFileSize = physicalBlockOneSize + physicalBlockTwoSize;
    List<Long> blockIds = new ArrayList<>();
    long blockOneId = 1L;
    long blockTwoId = 2L;
    blockIds.add(blockOneId);
    blockIds.add(blockTwoId);
    List<FileBlockInfo> fileBlockInfos = new ArrayList<>();
    fileBlockInfos.add(new FileBlockInfo().setBlockInfo(
        new BlockInfo().setBlockId(blockOneId).setLength(physicalBlockOneSize)));
    fileBlockInfos.add(new FileBlockInfo().setBlockInfo(
        new BlockInfo().setBlockId(blockTwoId).setLength(physicalBlockTwoSize)));

    FileInfo fileInfo = new FileInfo()
        .setEncrypted(true)
        .setFileId(Constants.INVALID_ENCRYPTION_ID)
        .setLength(physicalFileSize)
        .setBlockSizeBytes(mPhysicalBlockSize)
        .setBlockIds(blockIds)
        .setFileBlockInfos(fileBlockInfos);

    FileInfo converted = LayoutUtils.convertFileInfoToLogical(fileInfo, mMeta);
    Assert.assertEquals(logicalFileLength, converted.getLength());
    Assert.assertEquals(mLogicalBlockSize, converted.getBlockSizeBytes());
    Assert.assertEquals(logicalBlockSizeOne,
        (converted.getFileBlockInfos().get(0)).getBlockInfo().getLength());
    Assert.assertEquals(logicalBlockSizeTwo,
        (converted.getFileBlockInfos().get(1)).getBlockInfo().getLength());
  }

  @Test
  public void convertFileInfoToLogicalWithSplitFooter() throws Exception {
    long footerSize = mMeta.getEncodedMetaSize() + LayoutUtils.getFooterFixedOverhead();
    long logicalBlockSizeOne = mLogicalBlockSize - 2;
    long logicalFileLength = logicalBlockSizeOne;
    long physicalBlockOneSize = (mPhysicalBlockSize - 2) /* data */ + 2 /* partial file footer */;
    long physicalBlockTwoSize = footerSize - 2 /* second part of file footer */;
    long physicalFileSize = physicalBlockOneSize + physicalBlockTwoSize;
    List<Long> blockIds = new ArrayList<>();
    long blockOneId = 1L;
    long blockTwoId = 2L;
    blockIds.add(blockOneId);
    blockIds.add(blockTwoId);
    List<FileBlockInfo> fileBlockInfos = new ArrayList<>();
    fileBlockInfos.add(new FileBlockInfo().setBlockInfo(
        new BlockInfo().setBlockId(blockOneId).setLength(physicalBlockOneSize)));
    fileBlockInfos.add(new FileBlockInfo().setBlockInfo(
        new BlockInfo().setBlockId(blockTwoId).setLength(physicalBlockTwoSize)));

    FileInfo fileInfo = new FileInfo()
        .setEncrypted(true)
        .setFileId(Constants.INVALID_ENCRYPTION_ID)
        .setLength(physicalFileSize)
        .setBlockSizeBytes(mPhysicalBlockSize)
        .setBlockIds(blockIds)
        .setFileBlockInfos(fileBlockInfos);

    FileInfo converted = LayoutUtils.convertFileInfoToLogical(fileInfo, mMeta);
    Assert.assertEquals(logicalFileLength, converted.getLength());
    Assert.assertEquals(mLogicalBlockSize, converted.getBlockSizeBytes());
    Assert.assertEquals(1, converted.getFileBlockInfos().size());
    Assert.assertEquals(logicalBlockSizeOne,
        (converted.getFileBlockInfos().get(0)).getBlockInfo().getLength());
  }
}
