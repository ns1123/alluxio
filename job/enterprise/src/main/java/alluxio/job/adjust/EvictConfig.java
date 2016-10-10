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
 * Configuration of a job evicting a block.
 */
@ThreadSafe
@JsonTypeName(EvictConfig.NAME)
public final class EvictConfig implements JobConfig {
  private static final long serialVersionUID = 931006961650512841L;
  public static final String NAME = "Evict";

  /** Which block to evict. */
  private long mBlockId;

  /** How many replicas to evict. */
  private int mReplicaChange;

  /**
   * Constructs the configuration for Replicate job.
   *
   * @param blockId id of the block to adjust (or evict)
   * @param replicaChange number of replicas to add
   */
  @JsonCreator
  public EvictConfig(@JsonProperty("blockId") long blockId,
      @JsonProperty("replicaChange") int replicaChange) {
    Preconditions.checkArgument(replicaChange != 0, "Evict zero replica.");
    mBlockId = blockId;
    mReplicaChange = replicaChange;
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * @return the block ID for this replication job
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return how many more blocks to adjust of the target block
   */
  public int getReplicaChange() {
    return mReplicaChange;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof EvictConfig)) {
      return false;
    }
    EvictConfig that = (EvictConfig) obj;
    return Objects.equal(mBlockId, that.mBlockId) && Objects.equal(
        mReplicaChange, that.mReplicaChange);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId, mReplicaChange);
  }

}
