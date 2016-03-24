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
import alluxio.client.WriteType;
import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import jersey.repackaged.com.google.common.base.Preconditions;

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
   *        keep the original caching and persistence levels
   * @param overwrite whether an existing file should be overwritten
   */
  public MoveConfig(@JsonProperty("source") String source, @JsonProperty("destination") String dst,
      @JsonProperty("writeType") String writeType, @JsonProperty("overwrite") boolean overwrite) {
    mSource = new AlluxioURI(Preconditions.checkNotNull(source, "source must be set"));
    mDestination = new AlluxioURI(Preconditions.checkNotNull(dst, "destination must be set"));
    mWriteType = writeType == null ? null : WriteType.valueOf(writeType);
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
   * @return the writeType, possibly null if it wasn't set
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
    return mSource.equals(that.mSource) && mDestination.equals(that.mDestination);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSource, mDestination);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("source", mSource)
        .add("destination", mDestination)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
