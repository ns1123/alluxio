package alluxio.job.util;

import alluxio.Constants;

import org.junit.Assert;
import org.junit.Test;

/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Tests TimeSeries class.
 */
public final class TimeSeriesTest {
  private long mBase = 1234569L * Constants.SECOND_NANO;

  /**
   * Tests {@link TimeSeries#record(long)}.
   */
  @Test
  public void recordTest() {
    TimeSeries timeSeries = new TimeSeries();
    timeSeries.record(mBase + 10L * Constants.SECOND_NANO);
    timeSeries.record(mBase + 10L * Constants.SECOND_NANO + 1);
    timeSeries.record(mBase + 10L * Constants.SECOND_NANO + 2);
    timeSeries.record((mBase + 13L * Constants.SECOND_NANO));

    Assert.assertEquals(3, timeSeries.get(mBase + Constants.SECOND_NANO * 10L + 3));
    Assert.assertEquals(1, timeSeries.get(mBase + Constants.SECOND_NANO * 13L));
    Assert.assertEquals(0, timeSeries.get(mBase + Constants.SECOND_NANO * 12L));
    Assert.assertEquals(0, timeSeries.get(mBase + Constants.SECOND_NANO * 11L));
  }

  /**
   * Tests {@link TimeSeries#print(PrintStream)}.
   */
  @Test
  public void printTest() {
    TimeSeries timeSeries = new TimeSeries();
    timeSeries.record(mBase);
    timeSeries.record(mBase + 8L * Constants.SECOND_NANO);
    timeSeries.record(mBase + 8L * Constants.SECOND_NANO + 1);
    timeSeries.record(mBase + 9L * Constants.SECOND_NANO + 1);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);

    timeSeries.print(printStream);

    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format("Time series starts at %d with width %d.%n", mBase, Constants.SECOND_NANO));
    sb.append(String.format("%d %d%n", 0, 1));
    for (int i = 1; i < 8; i++) {
      sb.append(String.format("%d %d%n", i, 0));
    }
    sb.append(String.format("%d %d%n", 8, 2));
    sb.append(String.format("%d %d%n", 9, 1));
    printStream.close();

    Assert.assertEquals(sb.toString(), outputStream.toString());
  }

  /**
   * Tests {@link TimeSeries#sparsePrint(PrintStream)}.
   */
  @Test
  public void sparsePrintTest() {
    TimeSeries timeSeries = new TimeSeries();
    timeSeries.record(mBase);
    timeSeries.record(mBase + 8L * Constants.SECOND_NANO);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);
    timeSeries.sparsePrint(printStream);

    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format("Time series starts at %d with width %d.%n", mBase, Constants.SECOND_NANO));
    sb.append(String.format("%d %d%n", 0, 1));
    sb.append(String.format("%d %d%n", 8, 1));

    Assert.assertEquals(sb.toString(), outputStream.toString());
  }
}
