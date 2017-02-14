/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.exception;

import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A class for representing an error in getting a job config. This is used when a job config is
 * requested, but it could not be provided due to an error.
 */
public class ErrorConfig implements JobConfig {
  private static final long serialVersionUID = 1761278556746530063L;

  private String mMessage;

  public ErrorConfig(@JsonProperty String message) {
    mMessage = message;
  }

  @Override
  public String getName() {
    return "ErrorConfig";
  }

  public String getMessage() {
    return mMessage;
  }
}
