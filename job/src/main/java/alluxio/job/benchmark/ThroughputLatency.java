/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.Constants;
import alluxio.job.util.TimeSeries;

import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Records the throughput and latency information of benchmark operations.
 * Throughput is recorded as a time series, i.e. number of successful operations per unit of time.
 * Latency is recorded as a histogram of operation latency.
 */
public class ThroughputLatency implements BenchmarkTaskResult {
  private static final long serialVersionUID = -6333962882319595353L;

  // The histogram can record values between 1 microsecond and 1 min.
  private static final long HISTOGRAM_MAX_VALUE = 60000000L;
  // Value quantization will be no larger than 1/10^3 = 0.1%.
  private static final int HISTOGRAM_PRECISION = 3;

  // The unit of buckets in the throughput histogram in nano seconds.
  private static final long THROUGHPUT_UNIT_NANO = Constants.SECOND_NANO;
  // The unit of buckets in the latency histogram in nano seconds.
  private static final long LATENCY_UNIT_NANO = 1000L;

  private TimeSeries mThroughput;
  private Histogram mLatency;
  // The number of errors.
  // TODO(peis): We should break down by error types in the future.
  private long mError;
  // The total number of operations.
  private long mTotal;

  /**
   * Creates a new ThroughputLatency instance.
   */
  public ThroughputLatency() {
    mThroughput = new TimeSeries(THROUGHPUT_UNIT_NANO);
    mLatency = new Histogram(HISTOGRAM_MAX_VALUE, HISTOGRAM_PRECISION);
  }

  /**
   * @return the throughput histogram
   */
  public TimeSeries getThroughput() {
    return mThroughput;
  }

  /**
   * @return the latency histogram
   */
  public Histogram getLatency() {
    return mLatency;
  }

  /**
   * Add one record to histograms.
   * This is the only method in this class that can be called in multiple threads.
   *
   * @param startTimeNano the start time
   * @param endTimeNano the end time
   * @param success whether the execution is successful
   */
  public synchronized void record(long startTimeNano, long endTimeNano, boolean success) {
    mThroughput.record(endTimeNano);
    mLatency.recordValue((endTimeNano - startTimeNano) / LATENCY_UNIT_NANO);
    if (!success) {
      mError++;
    }
    mTotal++;
  }

  /**
   * Add two {@link ThroughputLatency} instances.
   *
   * @param other the other {@link ThroughputLatency} instance
   */
  public void add(ThroughputLatency other) {
    mLatency.add(other.getLatency());
    mThroughput.add(other.getThroughput());
  }

  /**
   * Output the stats a print stream.
   *
   * @param printStream the print stream
   */
  public void output(PrintStream printStream) {
    printStream.println("Number of errors: " + mError + "/" + mTotal);
    printStream.println("Latency.");
    mLatency.outputPercentileDistribution(printStream, 1.);
    printStream.println("Throughput.");
    mThroughput.print(printStream);
  }

  @Override
  public String toString() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);

    output(printStream);
    printStream.close();
    try {
      outputStream.close();
    } catch (IOException e) {
      // This should never happen.
      throw new RuntimeException(e);
    }
    return outputStream.toString();
  }
}

