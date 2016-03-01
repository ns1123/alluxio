/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.jobmanager.job;

import alluxio.exception.ExceptionMessage;
import alluxio.jobmanager.exception.JobDoesNotExistException;
import alluxio.jobmanager.job.persist.DistributedPersistConfig;
import alluxio.jobmanager.job.persist.DistributedPersistDefinition;
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

    add(DistributedPersistConfig.class, new DistributedPersistDefinition());
    add(DistributedPrefetchingConfig.class, new DistributedPrefetchingDefinition());
  }

  /**
   * Adds a mapping from the job configuration to the definition
   */
  private <T extends JobConfig> void add(Class<T> jobConfig, JobDefinition<T, ?> definition) {
    mJobConfigToDefinition.put(jobConfig, definition);
  }

  /**
   * Gets the job definition from the job configuration.
   *
   * @param jobConfig the job configuration
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
