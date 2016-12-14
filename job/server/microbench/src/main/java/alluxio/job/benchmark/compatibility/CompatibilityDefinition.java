/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.compatibility;

import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.AbstractNoArgBenchmarkJobDefinition;
import alluxio.job.benchmark.IOThroughputResult;
import alluxio.job.benchmark.ReportFormatUtils;
import alluxio.job.benchmark.compatibility.operations.CreateOperation;
import alluxio.job.benchmark.compatibility.operations.DeleteOperation;
import alluxio.job.benchmark.compatibility.operations.MountOperation;
import alluxio.job.benchmark.compatibility.operations.RenameOperation;
import alluxio.job.benchmark.compatibility.operations.SetAttributeOperation;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A simple benchmark generates and validates operations. This is useful for testing
 * compatibility of different versions of Alluxio.
 */
public final class CompatibilityDefinition
    extends AbstractNoArgBenchmarkJobDefinition<CompatibilityConfig, IOThroughputResult> {
  private List<Operation> mOperations;

  /**
   * Constructs a new {@link CompatibilityDefinition}.
   */
  public CompatibilityDefinition() {
    mOperations = new ArrayList<>();
  }

  @Override
  public String join(CompatibilityConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
      throws Exception {
    return ReportFormatUtils.createThroughputResultReport(config, taskResults, "Dummy");
  }

  @Override
  protected void before(CompatibilityConfig config,
      JobWorkerContext jobWorkerContext) throws Exception {
    mOperations = new ArrayList<>();
    mOperations.add(new CreateOperation(jobWorkerContext));
    mOperations.add(new DeleteOperation(jobWorkerContext));
    mOperations.add(new RenameOperation(jobWorkerContext));
    mOperations.add(new MountOperation(jobWorkerContext));
    mOperations.add(new SetAttributeOperation(jobWorkerContext));
  }

  @Override
  protected void run(CompatibilityConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext, int batch, int threadIndex) throws Exception {
    if (config.getGenerate()) {
      for (Operation operation : mOperations) {
        operation.generate();
      }
    }
    for (Operation operation : mOperations) {
      operation.validate();
    }
  }

  @Override
  protected void after(CompatibilityConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Do nothing
  }

  @Override
  protected IOThroughputResult process(CompatibilityConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    // Dummy values
    return new IOThroughputResult(1.0, 1);
  }

  @Override
  public Class<CompatibilityConfig> getJobConfigClass() {
    return CompatibilityConfig.class;
  }
}
