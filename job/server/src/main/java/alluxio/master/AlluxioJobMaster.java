/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.ServerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Entry point for the Alluxio job master program.
 */
@NotThreadSafe
public final class AlluxioJobMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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

    AlluxioJobMasterService master = AlluxioJobMasterService.Factory.create();
    ServerUtils.run(master, "Alluxio job master");
  }

  private AlluxioJobMaster() {} // prevent instantiation
}
