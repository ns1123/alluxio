/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.maxfile;

import alluxio.job.benchmark.AbstractBenchmarkJobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * The configuration for MaxFileDefinition.
 */
@JsonTypeName(MaxFileConfig.NAME)
public class MaxFileConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 1L;

  public static final String NAME = "MaxFile";

  /**
   * Creates an instance of MaxFileConfig.
   * @param threadNum the number of client threads
   */
  public MaxFileConfig(@JsonProperty("threadNum") int threadNum) {
    super(threadNum, 1, "ALLUXIO", true, false);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    return sb.toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
