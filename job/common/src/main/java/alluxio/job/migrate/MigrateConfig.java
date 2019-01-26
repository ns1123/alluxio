/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.migrate;

import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration for the migrate job. A migration can either be a copy or a move.
 * See {@code MigrateDefinition} for detailed semantics.
 */
@ThreadSafe
public class MigrateConfig implements JobConfig {
  private static final long serialVersionUID = 8014674802258120190L;
  private static final String NAME = "Migrate";

  private final String mSource;
  private final String mDestination;
  private final String mWriteType;
  private final boolean mOverwrite;
  private final boolean mDeleteSource;

  /**
   * @param source the source path
   * @param dst the destination path
   * @param writeType the Alluxio write type with which to write the migrated file; a null value means
   *        to use the default write type from the Alluxio configuration
   * @param overwrite whether an existing file should be overwritten; if the source and destination
   *        are directories, the contents of the directories will be merged with common files
   *        overwritten by the source
   * @param deleteSource whether to delete the source file or the entire source directory
   */
  public MigrateConfig(@JsonProperty("source") String source, @JsonProperty("destination") String dst,
                       @JsonProperty("writeType") String writeType,
                       @JsonProperty("overwrite") boolean overwrite,
                       @JsonProperty("deleteSource") boolean deleteSource) {
    mSource = Preconditions.checkNotNull(source, "source must be set");
    mDestination = Preconditions.checkNotNull(dst, "destination must be set");
    mWriteType = writeType;
    mOverwrite = overwrite;
    mDeleteSource = deleteSource;
  }

  /**
   * @return the source path
   */
  public String getSource() {
    return mSource;
  }

  /**
   * @return the destination path
   */
  public String getDestination() {
    return mDestination;
  }

  /**
   * @return the writeType
   */
  public String getWriteType() {
    return mWriteType;
  }

  /**
   * @return whether to overwrite a file at the destination if it exists
   */
  public boolean isOverwrite() {
    return mOverwrite;
  }

  /**
   * @return whether to delete the source file or the entire source directory
   */
  public boolean isDeleteSource() {
    return mDeleteSource;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MigrateConfig)) {
      return false;
    }
    MigrateConfig that = (MigrateConfig) obj;
    return Objects.equal(mSource, that.mSource)
        && Objects.equal(mDestination, that.mDestination)
        && Objects.equal(mWriteType, that.mWriteType)
        && Objects.equal(mOverwrite, that.mOverwrite)
        && Objects.equal(mDeleteSource, that.mDeleteSource);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSource, mDestination, mWriteType, mOverwrite, mDeleteSource);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("source", mSource)
        .add("destination", mDestination)
        .add("writeType", mWriteType)
        .add("overwrite", mOverwrite)
        .add("deleteSource", mDeleteSource)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
