/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.persist;

import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The configuration of persisting a file.
 */
@ThreadSafe
@JsonTypeName(PersistConfig.NAME)
public class PersistConfig implements JobConfig {
  private static final long serialVersionUID = -404303102995033014L;

  public static final String NAME = "Persist";

  private String mFilePath;
  /** configures if overwrite the existing file in under storage. */
  private final boolean mOverwrite;
  private String mSuffix;

  /**
   * Creates a new instance of {@link PersistConfig}.
   *
   * @param filePath the path of the file for persistence
   * @param overwrite flag of overwriting the existing file in under storage or not
   */
  @JsonCreator
  public PersistConfig(@JsonProperty("filePath") String filePath,
      @JsonProperty("overwrite") boolean overwrite,
      @JsonProperty("suffix") String suffix) {
    mFilePath = Preconditions.checkNotNull(filePath, "The file path cannot be null");
    mOverwrite = overwrite;
    mSuffix = suffix;
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * @return the file path
   */
  public String getFilePath() {
    return mFilePath;
  }

  /**
   * @return flag of overwriting the existing file in under storage or not
   */
  public boolean isOverwrite() {
    return mOverwrite;
  }

  /**
   * @return the suffix to use
   */
  public String getSuffix() {
    return mSuffix;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof PersistConfig)) {
      return false;
    }
    PersistConfig that = (PersistConfig) obj;
    return Objects.equal(mFilePath, that.mFilePath)
        && Objects.equal(mOverwrite, that.mOverwrite)
        && Objects.equal(mSuffix, that.mSuffix);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFilePath, mOverwrite, mSuffix);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("filePath", mFilePath)
        .add("overwrite", mOverwrite)
        .add("suffix", mSuffix)
        .toString();
  }
}
