/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.Configuration;
import alluxio.ProcessUtils;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.util.ConfigurationUtils;

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

    if (!ConfigurationUtils.masterHostConfigured()) {
      System.out.println(String.format(
          "Cannot run alluxio job master; master hostname is not "
              + "configured. Please modify %s to either set %s or configure zookeeper with "
              + "%s=true and %s=[comma-separated zookeeper master addresses]",
          Configuration.SITE_PROPERTIES, PropertyKey.MASTER_HOSTNAME.toString(),
          PropertyKey.ZOOKEEPER_ENABLED.toString(), PropertyKey.ZOOKEEPER_ADDRESS.toString()));
      System.exit(1);
    }

    JobMasterProcess process = JobMasterProcess.Factory.create();
    ProcessUtils.run(process);
  }

  private AlluxioJobMaster() {} // prevent instantiation
}
