/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.move;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The configuration of performing a move.
 */
@ThreadSafe
public class MoveConfig implements JobConfig {
  public static final String NAME = "Move";

  private static final long serialVersionUID = 2249161137868881346L;

  private final AlluxioURI mSource;
  private final AlluxioURI mDestination;
  private final WriteType mWriteType;
  private final boolean mOverwrite;

  /**
   * @param source the source path
   * @param dst the destination path
   * @param writeType the Alluxio write type with which to write the moved file; a null value means
   *        to use the default write type from the Alluxio configuration
   * @param overwrite whether an existing file should be overwritten; this will not overwrite
   *        directories
   */
  public MoveConfig(@JsonProperty("source") String source, @JsonProperty("destination") String dst,
      @JsonProperty("writeType") String writeType, @JsonProperty("overwrite") boolean overwrite) {
    mSource = new AlluxioURI(Preconditions.checkNotNull(source, "source must be set"));
    mDestination = new AlluxioURI(Preconditions.checkNotNull(dst, "destination must be set"));
    mWriteType = writeType == null
        ? Configuration.getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class)
        : WriteType.valueOf(writeType);
    mOverwrite = overwrite;
  }

  /**
   * @return the source path
   */
  public AlluxioURI getSource() {
    return mSource;
  }

  /**
   * @return the destination path
   */
  public AlluxioURI getDestination() {
    return mDestination;
  }

  /**
   * @return the writeType
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @return whether to overwrite a file at the destination if it exists
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
    if (!(obj instanceof MoveConfig)) {
      return false;
    }
    MoveConfig that = (MoveConfig) obj;
    return Objects.equal(mSource, that.mSource)
        && Objects.equal(mDestination, that.mDestination)
        && Objects.equal(mWriteType, that.mWriteType)
        && Objects.equal(mOverwrite, that.mOverwrite);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSource, mDestination, mWriteType, mOverwrite);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("source", mSource)
        .add("destination", mDestination)
        .add("writeType", mWriteType)
        .add("overwrite", mOverwrite)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
