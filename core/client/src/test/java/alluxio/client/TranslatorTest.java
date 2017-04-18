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

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Unit tests for {@link Translator}.
 */
public final class TranslatorTest {
  private static final int BLOCK_HEADER_SIZE = 128;
  private static final int BLOCK_FOOTER_SIZE = 64;
  private static final int LOGICAL_BLOCK_SIZE = Constants.MB;
  private static final int CHUNK_SIZE = Constants.DEFAULT_CHUNK_SIZE;
  private static final int CHUNK_HEADER_SIZE = 32;
  private static final int CHUNK_FOOTER_SIZE = Constants.DEFAULT_CHUNK_FOOTER_SIZE;

  private Translator mTranslator = new Translator(BLOCK_HEADER_SIZE, BLOCK_FOOTER_SIZE,
      LOGICAL_BLOCK_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE, CHUNK_FOOTER_SIZE);

  @Test
  public void basic() throws Exception {
    Assert.assertEquals(BLOCK_HEADER_SIZE, mTranslator.getBlockHeaderSize());
    Assert.assertEquals(BLOCK_FOOTER_SIZE, mTranslator.getBlockFooterSize());
    Assert.assertEquals(LOGICAL_BLOCK_SIZE, mTranslator.getLogicalBlockSize());
    Assert.assertEquals(CHUNK_HEADER_SIZE, mTranslator.getChunkHeaderSize());
    Assert.assertEquals(CHUNK_SIZE, mTranslator.getChunkSize());
    Assert.assertEquals(CHUNK_FOOTER_SIZE, mTranslator.getChunkFooterSize());
  }

  @Test
  public void translateLogicalOffsetToPhysical() throws Exception {
    final int physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;
    final int[] logicalOffset = new int[]{
        1,
        10,
        CHUNK_SIZE,
        CHUNK_SIZE + 10,
        CHUNK_SIZE + CHUNK_SIZE - 1,
        CHUNK_SIZE + CHUNK_SIZE,
        CHUNK_SIZE + CHUNK_SIZE + CHUNK_SIZE - 10,
    };
    final int[] physicalChunkStart = new int[] {
        BLOCK_HEADER_SIZE,
        BLOCK_HEADER_SIZE,
        BLOCK_HEADER_SIZE + physicalChunkSize,
        BLOCK_HEADER_SIZE + physicalChunkSize,
        BLOCK_HEADER_SIZE + physicalChunkSize,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize,
        BLOCK_HEADER_SIZE + physicalChunkSize + physicalChunkSize,
    };
    final int[] physicalChunkOffsetFromChunkStart = new int[] {
        CHUNK_HEADER_SIZE + 1,
        CHUNK_HEADER_SIZE + 10,
        CHUNK_HEADER_SIZE,
        CHUNK_HEADER_SIZE + 10,
        CHUNK_HEADER_SIZE + CHUNK_SIZE - 1,
        CHUNK_HEADER_SIZE,
        CHUNK_HEADER_SIZE + CHUNK_SIZE - 10,
    };
    final int[] physicalOffset = new int[] {
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
          mTranslator.logicalOffsetToPhysicalChunkStart(logicalOffset[i]));
      Assert.assertEquals(physicalChunkOffsetFromChunkStart[i],
          mTranslator.logicalOffsetToPhysicalFromChunkStart(logicalOffset[i]));
      Assert.assertEquals(physicalOffset[i], mTranslator.logicalOffsetToPhysical(logicalOffset[i]));
    }
  }

  @Test
  public void translatePhysicalOffsetToLogical() throws Exception {
    final int physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;
    final int[] logicalOffset = new int[] {
        1,
        10,
        CHUNK_SIZE,
        CHUNK_SIZE + 10,
        CHUNK_SIZE + CHUNK_SIZE - 1,
        CHUNK_SIZE + CHUNK_SIZE,
        CHUNK_SIZE + CHUNK_SIZE + CHUNK_SIZE - 10,
    };
    final int[] physicalOffset = new int[] {
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
      Assert.assertEquals(logicalOffset[i], mTranslator.physicalOffsetToLogical(physicalOffset[i]));
    }
  }

  @Test
  public void translateLogicalLengthToPhysical() throws Exception {
    class TestCase {
      int mLogicalOffset;
      int mLogicalLength;
      int mExpected;

      public TestCase(int expected, int logicalOffset, int logicalLength) {
        mExpected = expected;
        mLogicalOffset = logicalOffset;
        mLogicalLength = logicalLength;
      }
    }

    final int physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, 0, 0));
    testCases.add(new TestCase(1, 0, 1));
    testCases.add(new TestCase(CHUNK_SIZE / 2, 0, CHUNK_SIZE / 2));
    testCases.add(new TestCase(CHUNK_SIZE + CHUNK_FOOTER_SIZE, 0, CHUNK_SIZE));
    testCases.add(new TestCase(physicalChunkSize + 1, 0, CHUNK_SIZE + 1));
    testCases.add(new TestCase(0, CHUNK_SIZE / 2, 0));
    testCases.add(new TestCase(1, CHUNK_SIZE / 2, 1));
    testCases.add(new TestCase(CHUNK_SIZE / 2 + CHUNK_FOOTER_SIZE, CHUNK_SIZE / 2, CHUNK_SIZE / 2));
    testCases.add(new TestCase(physicalChunkSize, CHUNK_SIZE / 2, CHUNK_SIZE));
    testCases.add(new TestCase(physicalChunkSize + 1, CHUNK_SIZE / 2, CHUNK_SIZE + 1));
    testCases.add(new TestCase(physicalChunkSize + 1, CHUNK_SIZE, CHUNK_SIZE + 1));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with logical offset %d, logical length %d",
              testCase.mLogicalOffset, testCase.mLogicalLength),
          testCase.mExpected,
          mTranslator.logicalLengthToPhysical(testCase.mLogicalOffset, testCase.mLogicalLength));
    }
  }

  @Test
  public void translatePhysicalLengthToLogical() throws Exception {
    class TestCase {
      int mPhysicalOffset;
      int mPhysicalLength;
      int mExpected;

      public TestCase(int expected, int physicalOffset, int physicalLength) {
        mExpected = expected;
        mPhysicalOffset = physicalOffset;
        mPhysicalLength = physicalLength;
      }
    }

    final int physicalChunkSize = CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE;

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(0, BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE, 0));
    testCases.add(new TestCase(1, BLOCK_HEADER_SIZE + CHUNK_HEADER_SIZE, 1));
    testCases.add(new TestCase(10, BLOCK_HEADER_SIZE + physicalChunkSize / 2, 10));
    testCases.add(new TestCase(
        CHUNK_SIZE - (physicalChunkSize / 2 - CHUNK_HEADER_SIZE),
        BLOCK_HEADER_SIZE + physicalChunkSize / 2, physicalChunkSize / 2));
    testCases.add(new TestCase(
        CHUNK_SIZE, BLOCK_HEADER_SIZE + physicalChunkSize / 2, physicalChunkSize));
    testCases.add(new TestCase(
        CHUNK_SIZE - 1, BLOCK_HEADER_SIZE + physicalChunkSize / 2, physicalChunkSize - 1));
    testCases.add(new TestCase(
        10, BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE, 10));
    testCases.add(new TestCase(
        CHUNK_SIZE / 2, BLOCK_HEADER_SIZE + physicalChunkSize + CHUNK_HEADER_SIZE, CHUNK_SIZE / 2));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(
          String.format("Test failed with physical offset %d, physical length %d",
              testCase.mPhysicalOffset, testCase.mPhysicalLength),
          testCase.mExpected,
          mTranslator.physicalLengthToLogical(testCase.mPhysicalOffset, testCase.mPhysicalLength));
    }
  }
}
