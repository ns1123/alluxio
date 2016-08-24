/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.exception.ExceptionMessage;
import alluxio.job.benchmark.AsyncWriteConfig;
import alluxio.job.benchmark.AsyncWriteDefinition;
import alluxio.job.benchmark.FSMetaConfig;
import alluxio.job.benchmark.FSMetaDefinition;
import alluxio.job.benchmark.RemoteReadConfig;
import alluxio.job.benchmark.RemoteReadDefinition;
import alluxio.job.benchmark.SequentialWriteConfig;
import alluxio.job.benchmark.SequentialWriteDefinition;
import alluxio.job.benchmark.SimpleReadConfig;
import alluxio.job.benchmark.SimpleReadDefinition;
import alluxio.job.benchmark.SimpleWriteConfig;
import alluxio.job.benchmark.SimpleWriteDefinition;
import alluxio.job.benchmark.compatibility.CompatibilityConfig;
import alluxio.job.benchmark.compatibility.CompatibilityDefinition;
import alluxio.job.benchmark.huawei.HuaweiConfig;
import alluxio.job.benchmark.huawei.HuaweiDefinition;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.load.LoadConfig;
import alluxio.job.load.LoadDefinition;
import alluxio.job.move.MoveConfig;
import alluxio.job.move.MoveDefinition;
import alluxio.job.persist.PersistConfig;
import alluxio.job.persist.PersistDefinition;

import com.google.common.collect.Maps;

import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The central registry of all the job definitions.
 */
@ThreadSafe
public enum JobDefinitionRegistry {
  INSTANCE;

  private final Map<Class<?>, JobDefinition<?, ?, ?>> mJobConfigToDefinition;

  JobDefinitionRegistry() {
    mJobConfigToDefinition = Maps.newHashMap();

    add(AsyncWriteConfig.class, new AsyncWriteDefinition());
    add(CompatibilityConfig.class, new CompatibilityDefinition());
    add(FSMetaConfig.class, new FSMetaDefinition());
    add(HuaweiConfig.class, new HuaweiDefinition());
    add(LoadConfig.class, new LoadDefinition());
    add(MoveConfig.class, new MoveDefinition());
    add(PersistConfig.class, new PersistDefinition());
    add(RemoteReadConfig.class, new RemoteReadDefinition());
    add(SequentialWriteConfig.class, new SequentialWriteDefinition());
    add(SimpleReadConfig.class, new SimpleReadDefinition());
    add(SimpleWriteConfig.class, new SimpleWriteDefinition());
  }

  /**
   * Adds a mapping from the job configuration to the definition.
   */
  private <T extends JobConfig> void add(Class<T> jobConfig, JobDefinition<T, ?, ?> definition) {
    mJobConfigToDefinition.put(jobConfig, definition);
  }

  /**
   * Gets the job definition from the job configuration.
   *
   * @param jobConfig the job configuration
   * @param <T> the job configuration class
   * @return the job definition corresponding to the configuration
   * @throws JobDoesNotExistException when the job definition does not exist
   */
  @SuppressWarnings("unchecked")
  public synchronized <T extends JobConfig> JobDefinition<T, Object, Object> getJobDefinition(
      T jobConfig) throws JobDoesNotExistException {
    if (!mJobConfigToDefinition.containsKey(jobConfig.getClass())) {
      throw new JobDoesNotExistException(
          ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobConfig.getName()));
    }
    return (JobDefinition<T, Object, Object>) mJobConfigToDefinition.get(jobConfig.getClass());
  }

}
