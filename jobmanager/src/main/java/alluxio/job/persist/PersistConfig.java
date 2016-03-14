/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.persist;

import alluxio.AlluxioURI;
import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The configuration of persisting a file.
 */
@ThreadSafe
public class PersistConfig implements JobConfig {
  private static final long serialVersionUID = -404303102995033014L;

  public static final String NAME = "Persist";

  private final AlluxioURI mPath;

  /**
   * Creates a new instance of {@link PersistConfig}.
   *
   * @param path the path of the file for persistence
   */
  public PersistConfig(@JsonProperty("Path") String path) {
    mPath = new AlluxioURI(path);
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * @return the path to the file for persistence
   */
  public AlluxioURI getPath() {
    return mPath;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("path", mPath).toString();
  }
}
