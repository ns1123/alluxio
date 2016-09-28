/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import java.util.ArrayList;
import java.util.List;

/**
 * Records the runtime of the benchmark operation.
 */
public class RuntimeResult implements BenchmarkTaskResult {
  private static final long serialVersionUID = 438046697336454446L;
  private final List<Double> mRuntime = new ArrayList<>();

  /**
   * Creates a new instance of {@link RuntimeResult}.
   *
   * @param runtime the runtime
   */
  public RuntimeResult(List<Double> runtime) {
    mRuntime.addAll(runtime);
  }

  /**
   * @return the runtime of the operation
   */
  public List<Double> getRuntime() {
    return mRuntime;
  }
}
