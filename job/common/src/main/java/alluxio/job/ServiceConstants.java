/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

/**
 * Alluxio job service constants.
 */
public final class ServiceConstants {
  public static final String MASTER_SERVICE_PREFIX = "master/job";

  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";

  public static final String CANCEL_JOB = "cancel";
  public static final String LIST = "list";
  public static final String LIST_STATUS = "list_status";
  public static final String RUN_JOB = "run";

  private ServiceConstants() {} // prevent instantiation
}
