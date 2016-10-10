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
  private int mEvictNumber;

  /**
   * Constructs the configuration for an Evict job.
   *
   * @param blockId id of the block to evict
   * @param evictNumber number of replicas to evict
   */
  @JsonCreator
  public EvictConfig(@JsonProperty("blockId") long blockId,
      @JsonProperty("evictNumber") int evictNumber) {
    Preconditions.checkArgument(evictNumber >= 0, "evictNumber must be positive.");
    mBlockId = blockId;
    mEvictNumber = evictNumber;
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
   * @return how many existing blocks to evict
   */
  public int getEvictNumber() {
    return mEvictNumber;
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
    return Objects.equal(mBlockId, that.mBlockId) && Objects.equal(mEvictNumber, that.mEvictNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockId, mEvictNumber);
  }

}
