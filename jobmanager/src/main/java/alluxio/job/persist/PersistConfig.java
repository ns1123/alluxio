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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
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

  private final AlluxioURI mFilePath;
  /** configures if overwrite the existing file in under storage. */
  private final boolean mOverwrite;

  /**
   * Creates a new instance of {@link PersistConfig}.
   *
   * @param filePath the path of the file for persistence
   * @param overwrite flag of overwriting the existing file in under storage or not
   */
  @JsonCreator
  public PersistConfig(@JsonProperty("FilePath") String filePath,
      @JsonProperty("Overwrite") boolean overwrite) {
    mFilePath = new AlluxioURI(filePath);
    mOverwrite = overwrite;
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * @return the path to the file for persistence
   */
  @JsonIgnore
  public AlluxioURI getFilePath() {
    return mFilePath;
  }

  /**
   * @return the file path as a string
   */
  @JsonGetter("FilePath")
  public String getFilePathAsString() {
    return mFilePath.toString();
  }

  /**
   * @return flag of overwriting the existing file in under storage or not
   */
  @JsonGetter("Overwrite")
  public boolean isOverwrite() {
    return mOverwrite;
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
    return Objects.equal(mFilePath, that.mFilePath) && Objects.equal(mOverwrite, that.mOverwrite);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFilePath, mOverwrite);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("filePath", mFilePath).add("overwrite", mOverwrite)
        .toString();
  }
}
