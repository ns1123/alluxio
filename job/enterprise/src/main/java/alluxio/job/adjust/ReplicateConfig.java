/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.adjust;

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
  private final long mBlockId;

  /** How many replicas to make for this block. */
  private final int mReplicas;

  /** Path in Alluxio of the file of the block*/
  private final String mPath;

  /**
   * Constructs the configuration for a Replicate job.
   *
   * @param blockId id of the block to replicate
   * @param path path in Alluxio of the file of this block
   * @param replicas number of replicas to replicate
   */
  @JsonCreator
  public ReplicateConfig(@JsonProperty("blockId") long blockId, @JsonProperty("path") String path,
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
   * @return the block ID for this job
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return how many more replicas to create
   */
  public int getReplicas() {
    return mReplicas;
  }

  /**
   * @return the path of the file of the block
   */
  public String getPath() {
    return mPath;
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
    return Objects.equal(mBlockId, that.mBlockId) && Objects.equal(mPath, that.mPath) && Objects
        .equal(mReplicas, that.mReplicas);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId, mPath, mReplicas);
  }
}
