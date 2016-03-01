/*************************************************************************
* Copyright (c) 2016 Alluxio, Inc.  All rights reserved.
*
* This software and all information contained herein is confidential and
* proprietary to Alluxio, and is protected by copyright and other
* applicable laws in the United States and other jurisdictions.  You may
* not use, modify, reproduce, distribute, or disclose this software
* without the express written permission of Alluxio.
**************************************************************************/

package alluxio.jobmanager.job.prefetch;

import alluxio.AlluxioURI;
import alluxio.jobmanager.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The configuration of prefetching a single file in a distributed manner.
 */
@ThreadSafe
public class DistributedPrefetchingConfig implements JobConfig {
  public static final String NAME = "DistributedPrefetching";

  private static final long serialVersionUID = -7937106659935180792L;
  private AlluxioURI mFilePath;

  /**
   * @param filePath the file path
   */
  public DistributedPrefetchingConfig(@JsonProperty("FilePath") String filePath) {
    mFilePath = new AlluxioURI(filePath);
  }

  /**
   * @return the file path
   */
  public AlluxioURI getFilePath() {
    return mFilePath;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("FilePath", mFilePath).toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
