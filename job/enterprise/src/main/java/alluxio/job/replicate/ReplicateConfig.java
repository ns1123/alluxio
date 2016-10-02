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
 * Configuration of a block replication job.
 */
@ThreadSafe
@JsonTypeName(ReplicateConfig.NAME)
public final class ReplicateConfig implements JobConfig {
  private static final long serialVersionUID = 1807931900696165058L;
  public static final String NAME = "Replication";

  /** Which block to replicate. */
  private long mBlockId;

  /** How many more replicas to make for this block. */
  // TODO(Bin): support mNumReplicas to be negative --- eviction
  private int mNumReplicas;

  /**
   * Constructs the configuration for Replicate job.
   *
   * @param blockId id of the block to replicate (or evict)
   * @param numReplicas replicas, positive value for replicate and negative for evict
   */
  @JsonCreator
  public ReplicateConfig(@JsonProperty("blockId") long blockId,
      @JsonProperty("numReplicas") int numReplicas) {
    Preconditions.checkArgument(numReplicas != 0);
    mBlockId = blockId;
    mNumReplicas = numReplicas;
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * @return the block Id for this replication job
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return how many more blocks to replicate of the target block
   */
  public int getNumReplicas() {
    return mNumReplicas;
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
    return Objects.equal(mBlockId, that.mBlockId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId);
  }

}
