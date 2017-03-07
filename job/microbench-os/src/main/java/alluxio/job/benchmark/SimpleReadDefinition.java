/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.job.util.SerializableVoid;

/**
 * A simple instantiation of read micro benchmark {@link AbstractSimpleReadDefinition}.
 */
public final class SimpleReadDefinition
    extends AbstractSimpleReadDefinition<SimpleReadConfig, SerializableVoid> {
  /**
   * Constructs a new {@link SimpleReadDefinition}.
   */
  public SimpleReadDefinition() {}

  @Override
  public Class<SimpleReadConfig> getJobConfigClass() {
    return SimpleReadConfig.class;
  }
}
