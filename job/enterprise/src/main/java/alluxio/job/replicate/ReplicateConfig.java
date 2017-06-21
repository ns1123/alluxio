/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.replicate;

import alluxio.job.JobConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration of a job replicating a block.
 */
@ThreadSafe
@JsonTypeName(ReplicateConfig.NAME)
public final class ReplicateConfig implements JobConfig {
  private static final long serialVersionUID = 1807931900696165058L;
  public static final String NAME = "Replicate";

  /** Which block to replicate. */
  private long mBlockId;

  /** Alluxio path of the file to replicate. */
  private String mPath;

  /** How many replicas to make for this block. */
  private int mReplicas;

  /**
   * Creates a new instance of {@link ReplicateConfig}.
   *
   * @param path Alluxio path of the file whose block to replicate
   * @param blockId id of the block to replicate
   * @param replicas number of additional replicas to create
   */
  @JsonCreator
  public ReplicateConfig(@JsonProperty("path") String path, @JsonProperty("blockId") long blockId,
      @JsonProperty("replicas") int replicas) {
    Preconditions.checkArgument(replicas > 0, "replicas must be positive.");
    mBlockId = blockId;
    mPath = path;
    mReplicas = replicas;
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * @return the id of the block to replicate
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the path of the file whose block to replicate
   */
  public String getPath() {
    return mPath;
  }

  /**
   * @return number of additional replicas to create
   */
  public int getReplicas() {
    return mReplicas;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ReplicateConfig)) {
      return false;
    }
    ReplicateConfig that = (ReplicateConfig) obj;
    return Objects.equal(mBlockId, that.mBlockId)
        && Objects.equal(mPath, that.mPath)
        && Objects.equal(mReplicas, that.mReplicas);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId, mPath, mReplicas);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("blockId", mBlockId)
        .add("path", mPath)
        .add("replicas", mReplicas)
        .toString();
  }
}
