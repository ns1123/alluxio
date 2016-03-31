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
import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The configuration of performing a move.
 */
@ThreadSafe
public class MoveConfig implements JobConfig {
  public static final String NAME = "Move";

  private static final long serialVersionUID = 2249161137868881346L;

  private AlluxioURI mSrc;
  private AlluxioURI mDst;

  /**
   * @param src the source path
   * @param dst the destination path
   */
  public MoveConfig(@JsonProperty("src") String src, @JsonProperty("dst") String dst) {
    mSrc = new AlluxioURI(src);
    mDst = new AlluxioURI(dst);
  }

  /**
   * @return the source path
   */
  public AlluxioURI getSrc() {
    return mSrc;
  }

  /**
   * @return the destination path
   */
  public AlluxioURI getDst() {
    return mDst;
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
    return mSrc.equals(that.mSrc) && mDst.equals(that.mDst);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSrc, mDst);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("src", mSrc).add("dst", mDst).toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
