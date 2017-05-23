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

import org.junit.Assert;
import org.junit.Test;

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

  private static final String AES_GCM = "AES/GCM/NoPadding";
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
      .setEncryptionId(-1)
      .setPhysicalBlockSize(mPhysicalBlockSize)
      .build();

  private EncryptionProto.Meta mMeta = EncryptionProto.Meta.newBuilder()
      .setBlockHeaderSize(BLOCK_HEADER_SIZE)
      .setBlockFooterSize(BLOCK_FOOTER_SIZE)
      .setChunkHeaderSize(CHUNK_HEADER_SIZE)
      .setChunkSize(CHUNK_SIZE)
      .setChunkFooterSize(CHUNK_FOOTER_SIZE)
      .setFileId(-1)
      .setEncryptionId(-1)
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
        BLOCK_HEADER_SIZE,
        BLOCK_HEADER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize,
        BLOCK_HEADER_SIZE + physicalChunkSize,
        BLOCK_HEADER_SIZE + physicalChunkSize,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize,
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
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 1,
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 10,
        BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE + 10,
        BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE + CHUNK_SIZE - 1,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize + CHUNK_HEADER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize
            + CHUNK_HEADER_SIZE + CHUNK_SIZE - 10,
    };
    for (int i = 0; i < logicalOffset.length; i++) {
      Assert.assertEquals(physicalChunkStart[i],
          LayoutUtils.getPhysicalChunkStart(mMeta, logicalOffset[i]));
      Assert.assertEquals(physicalChunkOffsetFromChunkStart[i],
          LayoutUtils.getPhysicalOffsetFromChunkStart(mMeta, logicalOffset[i]));
      Assert.assertEquals(physicalOffset[i],
          LayoutUtils.toPhysicalOffset(mMeta, logicalOffset[i]));
    }
  }

  @Test
  public void translatePhysicalOffsetToLogical() throws Exception {
    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;
    final long[] logicalOffset = new long[] {
        0,
        0,
        1,
        10,
        CHUNK_SIZE,
        CHUNK_SIZE + 10,
        CHUNK_SIZE + CHUNK_SIZE - 1,
        CHUNK_SIZE + CHUNK_SIZE,
        CHUNK_SIZE + CHUNK_SIZE + CHUNK_SIZE - 10,
    };
    final long[] physicalOffset = new long[] {
        0,
        BLOCK_HEADER_SIZE,
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 1 + CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE + 10 + CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE + CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE + 10 + CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE + (CHUNK_SIZE - 1)
            + CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize + CHUNK_HEADER_SIZE
            + CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize
            + CHUNK_HEADER_SIZE + CHUNK_SIZE - 10 + CHUNK_FOOTER_SIZE,
    };
    for (int i = 0; i < logicalOffset.length; i++) {
      Assert.assertEquals(logicalOffset[i],
          LayoutUtils.toLogicalOffset(mMeta, physicalOffset[i]));
    }
  }

  @Test
  public void translateLogicalLengthToPhysical() throws Exception {
    class TestCase {
      long mExpected;
      long mLogicalOffset;
      long mLogicalLength;

      public TestCase(long expected, long logicalOffset, long logicalLength) {
        mExpected = expected;
        mLogicalOffset = logicalOffset;
        mLogicalLength = logicalLength;
      }
    }

    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(CHUNK_FOOTER_SIZE, 0, 0));
    testCases.add(new TestCase(1 + CHUNK_FOOTER_SIZE, 0, 1));
    testCases.add(new TestCase(CHUNK_SIZE / 2 + CHUNK_FOOTER_SIZE, 0, CHUNK_SIZE / 2));
    testCases.add(new TestCase(CHUNK_SIZE + CHUNK_FOOTER_SIZE, 0, CHUNK_SIZE));
    testCases.add(new TestCase(physicalChunkSize + 1 + CHUNK_FOOTER_SIZE, 0, CHUNK_SIZE + 1));
    testCases.add(new TestCase(CHUNK_FOOTER_SIZE, CHUNK_SIZE / 2, 0));
    testCases.add(new TestCase(1 + CHUNK_FOOTER_SIZE, CHUNK_SIZE / 2, 1));
    testCases.add(new TestCase(CHUNK_SIZE / 2 + CHUNK_FOOTER_SIZE, CHUNK_SIZE / 2, CHUNK_SIZE / 2));
    testCases.add(new TestCase(physicalChunkSize + CHUNK_FOOTER_SIZE, CHUNK_SIZE / 2, CHUNK_SIZE));
    testCases.add(new TestCase(
        physicalChunkSize + 1 + CHUNK_FOOTER_SIZE, CHUNK_SIZE / 2, CHUNK_SIZE + 1));
    testCases.add(new TestCase(
        physicalChunkSize + 1 + CHUNK_FOOTER_SIZE, CHUNK_SIZE, CHUNK_SIZE + 1));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with logical offset %d, logical length %d",
              testCase.mLogicalOffset, testCase.mLogicalLength),
          testCase.mExpected,
          LayoutUtils.toPhysicalLength(
              mMeta, testCase.mLogicalOffset, testCase.mLogicalLength));
    }
  }

  @Test
  public void translatePhysicalLengthToLogical() throws Exception {
    class TestCase {
      long mExpected;
      long mPhysicalOffset;
      long mPhysicalLength;

      public TestCase(long expected, long physicalOffset, long physicalLength) {
        mExpected = expected;
        mPhysicalOffset = physicalOffset;
        mPhysicalLength = physicalLength;
      }
    }

    final long physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, 0, CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(0, BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE, CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(1, BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE, 1 + CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(
        10, BLOCK_HEADER_SIZE + physicalChunkSize / 2, 10 + CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(
        physicalChunkSize / 2 - CHUNK_FOOTER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize / 2, physicalChunkSize / 2));
    testCases.add(new TestCase(
        CHUNK_SIZE, BLOCK_HEADER_SIZE + physicalChunkSize / 2,
        physicalChunkSize + CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(
        CHUNK_SIZE - 1, BLOCK_HEADER_SIZE + physicalChunkSize / 2,
        physicalChunkSize - 1 + CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(
        10, BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE, 10 + CHUNK_FOOTER_SIZE));
    testCases.add(new TestCase(
        CHUNK_SIZE / 2, BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE,
        CHUNK_SIZE / 2 + CHUNK_FOOTER_SIZE));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with physical offset %d, physical length %d",
              testCase.mPhysicalOffset, testCase.mPhysicalLength),
          testCase.mExpected,
          LayoutUtils.toLogicalLength(
              mMeta, testCase.mPhysicalOffset, testCase.mPhysicalLength));
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
    Assert.assertEquals(mMeta, LayoutUtils.decodeFooter(mMeta.getFileId(), encodedFooter, mKey));
  }

  @Test
  public void encodeAndDecodeWithFactoryCreatedMeta() throws Exception {
    EncryptionProto.Meta expected = EncryptionMetaFactory.create();
    byte[] encodedFooter = LayoutUtils.encodeFooter(expected);
    Assert.assertEquals(expected,
        LayoutUtils.decodeFooter(expected.getFileId(), encodedFooter, expected.getCryptoKey()));
  }
}
