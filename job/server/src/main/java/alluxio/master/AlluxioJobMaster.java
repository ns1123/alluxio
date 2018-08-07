/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.ProcessUtils;
import alluxio.RuntimeConstants;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Entry point for the Alluxio job master program.
 */
@ThreadSafe
public final class AlluxioJobMaster {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioJobMaster.class);

  /**
   * Starts the Alluxio job master.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioJobMaster.class.getCanonicalName());
      System.exit(-1);
    }

    CommonUtils.PROCESS_TYPE.set(alluxio.util.CommonUtils.ProcessType.JOB_MASTER);
    JobMasterProcess process;
    try {
      process = JobMasterProcess.Factory.create();
    } catch (Throwable t) {
      LOG.error("Failed to create job master process", t);
      // Exit to stop any non-daemon threads.
      System.exit(-1);
      throw t;
    }
    ProcessUtils.run(process);
  }

  private AlluxioJobMaster() {} // prevent instantiation
}