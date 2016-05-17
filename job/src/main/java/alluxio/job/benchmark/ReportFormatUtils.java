/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.wire.WorkerInfo;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Utility methods for reporting the benchmark result.
 */
public final class ReportFormatUtils {
  private ReportFormatUtils() {} // prevent instantiation

  /**
   * Creates the throughput result. If the report is not verbose, returns the average throughput.
   * Otherwise also includes the number per node.
   *
   * @param config the benchmark configuration
   * @param taskResults the task results
   * @return the verbose report
   */
  public static String createThroughputResultReport(AbstractBenchmarkJobConfig config,
      Map<WorkerInfo, IOThroughputResult> taskResults) {
    StringBuilder sb = new StringBuilder();
    double total = 0.0;
    double totalTime = 0;
    for (IOThroughputResult result : taskResults.values()) {
      total += result.getThroughput();
      totalTime += result.getDuration();
    }
    sb.append(String.format("Throughput:%s (MB/s)%n", getStringValue(total / taskResults.size())));
    sb.append(String.format("Duration:%f (ms)%n", totalTime / taskResults.size()));
    if (config.isVerbose()) {
      sb.append(String.format("********** Task Configurations **********%n"));
      sb.append(config.toString());
      sb.append(String.format("%n********** Statistics **********%n"));
      sb.append(String.format("%nWorker\t\tThroughput(MB/s)"));
      for (Entry<WorkerInfo, IOThroughputResult> entry : taskResults.entrySet()) {
        sb.append(entry.getKey().getId() + "@" + entry.getKey().getAddress().getHost());
        sb.append("\t\t" + getStringValue(entry.getValue().getThroughput()));
      }
    }
    return sb.toString();
  }

  /**
   * Gets the string value of the throughput. Also returns 0 if the value is smaller than the
   * epsilon.
   *
   * @param throughput the throughput
   * @return the string value
   */
  private static String getStringValue(double throughput) {
    if (Math.abs(throughput) < 2 * Double.MIN_VALUE) {
      return "0";
    }
    return throughput + "";
  }
}
