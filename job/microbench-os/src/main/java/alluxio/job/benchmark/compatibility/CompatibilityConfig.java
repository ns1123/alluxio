/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.compatibility;

import alluxio.job.benchmark.AbstractBenchmarkJobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;

/**
 * The configuration for the Compatibility benchmark job.
 */
@JsonTypeName(CompatibilityConfig.NAME)
public class CompatibilityConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = -7822302068495983496L;
  public static final String NAME = "Compatibility";

  private final boolean mGenerate;

  /**
   * Creates a new instance of {@link CompatibilityConfig}.
   *
   * @param generate if true, will generate operations, before validating the operations
   * @param verbose whether the report is verbose
   */
  public CompatibilityConfig(
      @JsonProperty("generate") boolean generate,
      @JsonProperty("verbose") boolean verbose) {
    super(1, 1, "ALLUXIO", verbose, false);
    mGenerate = generate;
  }

  /**
   * @return true if operations should be generated before validating
   */
  public boolean getGenerate() {
    return mGenerate;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("threadNum", getThreadNum())
        .add("batchNum", getBatchNum())
        .add("generate", mGenerate)
        .add("verbose", isVerbose())
        .add("cleanUp", isCleanUp())
        .toString();
  }
}
