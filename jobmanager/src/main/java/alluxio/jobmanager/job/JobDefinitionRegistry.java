/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.jobmanager.job;

import alluxio.exception.ExceptionMessage;
import alluxio.jobmanager.exception.JobDoesNotExistException;
import alluxio.jobmanager.job.prefetch.DistributedPrefetchingConfig;
import alluxio.jobmanager.job.prefetch.DistributedPrefetchingDefinition;

import com.google.common.collect.Maps;

import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The central registry of all the job definitions.
 */
@ThreadSafe
public enum JobDefinitionRegistry {
  INSTANCE;

  private final Map<Class<?>, JobDefinition<?, ?>> mJobConfigToDefinition;

  private JobDefinitionRegistry() {
    mJobConfigToDefinition = Maps.newHashMap();

    add(DistributedPrefetchingConfig.class, new DistributedPrefetchingDefinition());
  }

  /**
   * Adds a mapping from the job configuration to the definition.
   */
  private <T extends JobConfig> void add(Class<T> jobConfig, JobDefinition<T, ?> definition) {
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
  public synchronized <T extends JobConfig> JobDefinition<T, Object> getJobDefinition(T jobConfig)
      throws JobDoesNotExistException {
    if (!mJobConfigToDefinition.containsKey(jobConfig.getClass())) {
      throw new JobDoesNotExistException(
          ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobConfig.getName()));
    }
    return (JobDefinition<T, Object>) mJobConfigToDefinition.get(jobConfig.getClass());
  }

}
