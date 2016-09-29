/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The abstract class for all the benchmark job implementations. This class will launch multiple
 * threads for running the benchmark. It also times the execution per thread.
 *
 * @param <T> the benchmark job configuration
 * @param <R> the benchmark task result type
 */
public abstract class AbstractNoArgBenchmarkJobDefinition
    <T extends AbstractBenchmarkJobConfig, R extends BenchmarkTaskResult>
    extends AbstractBenchmarkJobDefinition<T, SerializableVoid, R> {
  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(T config, List<WorkerInfo> workerInfoList,
      JobMasterContext jobMasterContext) throws Exception {
    Map<WorkerInfo, SerializableVoid> result = new TreeMap<>(JobUtils.createWorkerInfoComparator());
    for (WorkerInfo workerInfo : workerInfoList) {
      result.put(workerInfo, (SerializableVoid) null);
    }
    return result;
  }

  @Override
  public R runTask(T config, SerializableVoid args, JobWorkerContext jobWorkerContext) throws Exception {
    return super.runTask(config, args, jobWorkerContext);
  }
}
