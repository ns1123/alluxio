/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.util;

import alluxio.Constants;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class to record a time series, e.g. traffic over time.
 */
@NotThreadSafe
public final class TimeSeries implements Serializable {
  private static final long serialVersionUID = -9139286113871170329L;

  private final long mWidthNano;
  private TreeMap<Long, Integer> mSeries = new TreeMap<>();

  /**
   * Creates a TimeSeries instance with given width.
   *
   * @param widthNano the granularity of the time series. If this is set to 1 min, we count
   *                  the number of events of every minute.
   */
  public TimeSeries(long widthNano) {
    mWidthNano = widthNano;
  }

  /**
   * Creates a TimeSeries instance with default width set to 1 second.
   */
  public TimeSeries() {
    mWidthNano = Constants.SECOND_NANO;
  }

  /**
   * Record one event at a timestamp into the time series.
   *
   * @param timeNano the time in nano seconds
   */
  public void record(long timeNano) {
    record(timeNano, 1);
  }

  /**
   * Record events at a timestamp into the time series.
   * @param timeNano the time in nano seconds
   * @param numEvents the number of events happened at timeNano
   */
  public void record(long timeNano, int numEvents) {
    long leftEndPoint = bucket(timeNano);
    mSeries.put(leftEndPoint,
        (mSeries.containsKey(leftEndPoint) ? mSeries.get(leftEndPoint) : 0) + numEvents);
  }

  /**
   * @param timeNano the time in nano seconds
   * @return the number of event happened in the bucket that includes timeNano
   */
  public int get(long timeNano) {
    long leftEndPoint = bucket(timeNano);
    return mSeries.containsKey(leftEndPoint) ? mSeries.get(leftEndPoint) : 0;
  }

  /**
   * @return the width
   */
  public long getWidthNano() {
    return mWidthNano;
  }

  /**
   * @return the whole time series
   */
  public TreeMap<Long, Integer> getSeries() {
    return mSeries;
  }

  /**
   * Add one histogram to the current one. We preserve the width in the current TimeSeries.
   *
   * @param other the TimeSeries instance to add
   */
  public void add(TimeSeries other) {
    TreeMap<Long, Integer> otherSeries = other.getSeries();
    for (Map.Entry<Long, Integer> event : otherSeries.entrySet()) {
      record(event.getKey() + other.getWidthNano() / 2, event.getValue());
    }
  }

  @Override
  public String toString() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);

    print(printStream);
    printStream.close();
    try {
      outputStream.close();
    } catch (IOException e) {
      // This should never happen.
      throw new RuntimeException(e);
    }
    return outputStream.toString();
  }

  /**
   * Print the time series sparsely, i.e. it ignores buckets with 0 events.
   *
   * @param stream the print stream
   */
  public void sparsePrint(PrintStream stream) {
    if (mSeries.isEmpty()) {
      return;
    }
    long start = mSeries.firstKey();
    stream.printf("Time series starts at %d with width %d.\n", start, mWidthNano);

    for (Map.Entry<Long, Integer> entry : mSeries.entrySet()) {
      stream.printf("%d %d\n", (entry.getKey() - start) / mWidthNano, entry.getValue());
    }
  }

  /**
   * Print the time series densely, i.e. it doesn't ignore buckets with 0 events.
   *
   * @param stream the print stream
   */
  public void print(PrintStream stream) {
    if (mSeries.isEmpty()) {
      return;
    }
    long start = mSeries.firstKey();
    stream.printf("Time series starts at %d with width %d.\n", start, mWidthNano);
    int bucketIndex = 0;
    Iterator<Map.Entry<Long, Integer>> it = mSeries.entrySet().iterator();

    Map.Entry<Long, Integer> current = it.next();
    while (current != null){
      int numEvents = 0;
      if (bucketIndex * mWidthNano + start == current.getKey()) {
        numEvents = current.getValue();
        current = null;
        if (it.hasNext()) {
          current = it.next();
        }
      }
      stream.printf("%d %d\n", bucketIndex, numEvents);
      bucketIndex++;
    };
  }

  /**
   * @param timeNano the time in nano seconds
   * @return the bucketed timestamp in nano seconds
   */
  private long bucket(long timeNano) {
    return timeNano / mWidthNano * mWidthNano;
  }
}
