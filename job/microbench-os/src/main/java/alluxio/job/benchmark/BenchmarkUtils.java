/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.util.CommonUtils;

import com.google.common.base.Function;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Utility methods for benchmarks.
 */
public final class BenchmarkUtils {

  /**
   * Waits for a condition to be satisfied until a timeout occurs.
   *
   * @param condition the condition to wait on
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   */
  public static void waitFor(Function<Void, Boolean> condition, long timeoutMs) {
    long start = System.currentTimeMillis();
    while (!condition.apply(null)) {
      if (System.currentTimeMillis() - start > timeoutMs) {
        throw new RuntimeException(String
            .format("Timed out waiting for condition %s after %d ms", condition.toString(),
                timeoutMs));
      }
      CommonUtils.sleepMs(20);
    }
  }

  /**
   * Writes a file to an output stream.
   *
   * @param os output stream to write
   * @param bufferSize buffer size in bytes
   * @param fileSize file size in bytes
   * @throws IOException if error happens
   */
  public static void writeFile(OutputStream os, long bufferSize, long fileSize) throws IOException {
    // write the file
    byte[] content = new byte[(int) bufferSize];
    Arrays.fill(content, (byte) 'a');
    long remain = fileSize;
    while (remain >= bufferSize) {
      os.write(content);
      remain -= bufferSize;
    }
    if (remain > 0) {
      os.write(content, 0, (int) remain);
    }
  }

  /**
   * Reads a file from an input stream.
   *
   * @param is input stream to read
   * @param bufferSize buffer size in bytes
   * @return number of bytes read
   * @throws IOException if error happens
   */
  public static long readFile(InputStream is, long bufferSize) throws IOException {
    byte[] content = new byte[(int) bufferSize];
    int lastReadSize = is.read(content);
    long readLen = lastReadSize;
    while (lastReadSize > 0) {
      lastReadSize = is.read(content);
      readLen += lastReadSize;
    }
    return readLen;
  }

  private BenchmarkUtils() {} // prevent instantiation
}
