/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The configuration of a dummy job for testing.
 */
@ThreadSafe
@JsonTypeName(TestJobConfig.NAME)
public class TestJobConfig implements JobConfig {
  public static final String NAME = "Test";

  private static final long serialVersionUID = -7937106659935180792L;
  private final String mFilePath;

  /**
   * @param filePath the file path
   */
  public TestJobConfig(@JsonProperty("filePath") String filePath) {
    mFilePath = Preconditions.checkNotNull(filePath, "The file path cannot be null");
  }

  /**
   * @return the file path
   */
  public String getFilePath() {
    return mFilePath;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TestJobConfig)) {
      return false;
    }
    TestJobConfig that = (TestJobConfig) obj;
    return mFilePath.equals(that.mFilePath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFilePath);
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
