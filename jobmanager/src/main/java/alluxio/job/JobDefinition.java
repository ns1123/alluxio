/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.wire.WorkerInfo;

import java.util.List;
import java.util.Map;

/**
 * A job definition. A definition has two important parts: (1) a
 * {@link JobDefinition#selectExecutors(JobConfig, List, JobMasterContext)} method runs at the
 * master node and selects the workers to run the executors. (2) a
 * {@link #runTask(JobConfig, Object, JobWorkerContext)}} method runs at each selected executor on
 * the worker node.
 *
 * @param <T> the job configuration
 * @param <P> the parameters to pass to each task
 * @param <R> the return type from the task
 */
public interface JobDefinition<T extends JobConfig, P, R> {

  /**
   * Selects the workers to run the task.
   *
   * @param config the job configuration
   * @param jobWorkerInfoList the list of available workers' information
   * @param jobMasterContext the context at the job manager master
   * @return a map of selected workers to the parameters to pass along
   * @throws Exception if any error occurs
   */
  Map<WorkerInfo, P> selectExecutors(T config, List<WorkerInfo> jobWorkerInfoList,
      JobMasterContext jobMasterContext) throws Exception;

  /**
   * Runs the task in the executor.
   *
   * @param config the job configuration
   * @param args the arguments passed in
   * @param jobWorkerContext the context at the job manager worker
   * @return the task result
   * @throws Exception if any error occurs
   */
  R runTask(T config, P args, JobWorkerContext jobWorkerContext) throws Exception;

  /**
   * Joins the task results on the master.
   *
   * @param config the job configuration
   * @param taskResults the task results
   * @return the joined results
   * @throws Exception if any error occurs
   */
  String join(T config, Map<WorkerInfo, R> taskResults) throws Exception;
}
