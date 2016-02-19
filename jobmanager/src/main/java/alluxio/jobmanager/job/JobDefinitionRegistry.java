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

import java.util.Map;

import com.google.common.collect.Maps;

import alluxio.jobmanager.job.persist.DistributedPersistConfig;
import alluxio.jobmanager.job.persist.DistributedPersistDefinition;
import alluxio.jobmanager.job.prefetch.DistributedPrefetchingConfig;
import alluxio.jobmanager.job.prefetch.DistributedPrefetchingDefinition;

public enum JobDefinitionRegistry {
  INSTANCE;

  private final Map<Class<?>, JobDefinition<?, ?>> mJobConfigToDefinition;

  private JobDefinitionRegistry() {
    mJobConfigToDefinition = Maps.newHashMap();

    add(DistributedPersistConfig.class, new DistributedPersistDefinition());
    add(DistributedPrefetchingConfig.class, new DistributedPrefetchingDefinition());
  }

  private <T extends JobConfig> void add(Class<T> jobConfig, JobDefinition<T, ?> definition) {
    mJobConfigToDefinition.put(jobConfig, definition);
  }

  public <T extends JobConfig> JobDefinition<T, Object> getJobDefinition(T jobConfig) {
    // TODO(yupeng) error check
    return (JobDefinition<T, Object>) mJobConfigToDefinition.get(jobConfig.getClass());
  }

}
