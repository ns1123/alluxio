/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FSMasterCreateDirConfig extends AbstractThroughputLatencyJobConfig {
  private static final long serialVersionUID = -3126766292453508529L;

  public static final String NAME = "FSMasterCreateDir";

  /**
   * Create a FSMasterCreateDirConfig instance.
   *
   * @param load the number of directories to create in total
   * @param expectedThroughput the rate at which to create directories
   * @param threadNum the number of client threads
   */
  public FSMasterCreateDirConfig(@JsonProperty("load") int load,
      @JsonProperty("throughput") double expectedThroughput,
      @JsonProperty("threadNum") int threadNum) {
    super(load, expectedThroughput, threadNum, true);
  }

  @Override
  public String getName() {
    return NAME;
  }
}