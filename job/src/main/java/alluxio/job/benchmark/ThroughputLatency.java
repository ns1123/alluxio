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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

/**
 * Records the throughput and latency information of benchmark operations.
 * Throughput is recorded as a time series, i.e. number of successful operations per unit of time.
 * Latency is recorded as a histogram of operation latency.
 */
public class ThroughputLatency implements BenchmarkTaskResult {
  private static final long serialVersionUID = -6333962882319595353L;

  // The histogram can record values between 1 microsecond and 1 hour.
  private static final long HISTOGRAM_MAX_VALUE = 1000000L * 60 * 60;
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
   *
   * @param startTimeNano the start time
   * @param endTimeNano the end time
   * @param success whether the execution is successful
   */
  public void record(long startTimeNano, long endTimeNano, boolean success) {
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
    mTotal += other.mTotal;
    mError += other.mError;
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

  /**
   * Outputs the benchmark entry representing the results of the run.
   *
   * @param tableName the database table name
   * @param comment the comment column containing the results data
   * @return the benchmark entry
   */
  public BenchmarkEntry createBenchmarkEntry(String tableName, String comment) {
    List<String> columnNames = ImmutableList.of("Throughput", "AverageThroughput", "PeakThroughput",
        "ThroughputStdDev", "Latency", "Latency50", "Latency90", "Latency99", "Latency99_9",
        "Latency99_99", "LatencyStdDev", "ErrorRatio", "Comment");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(baos);
    mLatency.outputPercentileDistribution(printStream, 1.);
    String latency = new String(baos.toByteArray(), Charsets.UTF_8);

    baos = new ByteArrayOutputStream();
    printStream = new PrintStream(baos);
    mThroughput.print(printStream);
    String throughput = new String(baos.toByteArray(), Charsets.UTF_8);
    TimeSeries.Summary summary = mThroughput.getSummary();

    ImmutableList.Builder<Object> builder = ImmutableList.builder();
    builder.add(throughput, summary.mMean, summary.mPeak, summary.mStddev);
    builder.add(latency, mLatency.getValueAtPercentile(50), mLatency.getValueAtPercentile(90),
        mLatency.getValueAtPercentile(99), mLatency.getValueAtPercentile(99.9),
        mLatency.getValueAtPercentile(99.99));
    builder.add(mLatency.getStdDeviation(), mError * 1.0 / mTotal, comment);
    List<Object> values = builder.build();

    ImmutableMap.Builder<String, Object> results = ImmutableMap.builder();
    for (int i = 0; i < columnNames.size(); i++) {
      results.put(columnNames.get(i), values.get(i));
    }
    return new BenchmarkEntry(tableName, columnNames,
        ImmutableList.of("text", "float", "float", "float", "text", "int", "int", "int", "int",
            "int", "float", "float", "text"),
        results.build());
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

