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
  /** Determines the UFS path to persist the file to. */
  private String mUfsPath;
  /** The mount ID for the UFS path to persist the file to. */
  private long mMountId;
  /** Determines whether to overwrite an existing file in UFS. */
  private final boolean mOverwrite;

  /**
   * Creates a new instance of {@link PersistConfig}.
   *
   * @param filePath the Alluxio path of the file to persist
   * @param ufsPath the UFS path to persist the file to
   * @param mountId the mount ID for the UFS path to persist the file to
   * @param overwrite flag of overwriting the existing file in UFS or not
   */
  @JsonCreator
  public PersistConfig(@JsonProperty("filePath") String filePath,
      @JsonProperty("ufsPath") String ufsPath, @JsonProperty("mountId") long mountId,
      @JsonProperty("overwrite") boolean overwrite) {
    mFilePath = Preconditions.checkNotNull(filePath, "The file path cannot be null");
    mUfsPath = Preconditions.checkNotNull(ufsPath, "The UFS path cannot be null");
    mMountId = Preconditions.checkNotNull(mountId, "The mount ID cannot be null");
    mOverwrite = overwrite;
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
   * @return the UFS path
   */
  public String getUfsPath() {
    return mUfsPath;
  }

  /**
   * @return the mount ID
   */
  public long getMountId() {
    return mMountId;
  }

  /**
   * @return flag of overwriting the existing file in under storage or not
   */
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
    return Objects.equal(mFilePath, that.mFilePath)
        && Objects.equal(mUfsPath, that.mUfsPath)
        && Objects.equal(mMountId, that.mMountId)
        && Objects.equal(mOverwrite, that.mOverwrite);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFilePath, mUfsPath, mMountId, mOverwrite);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("filePath", mFilePath).add("ufsPath", mUfsPath)
        .add("mountId", mMountId).add("overwrite", mOverwrite).toString();
  }
}
