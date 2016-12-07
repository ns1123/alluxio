/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

/**
 * Database constants.
 */
public final class DatabaseConstants {
  public static final String ASYNC_WRITE = "AsyncWrite";
  public static final String MAX_FILE = "MaxFile";
  public static final String MICROBENCH_DURATION_THROUGHPUT = "MicrobenchDurationThroughput";
  public static final String REPLICATION = "Replication";
  public static final String SEQUENTIAL_WRITE = "SequentialWrite";

  private DatabaseConstants() {} // prevent instantiation
}
