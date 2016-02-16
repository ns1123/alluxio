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

package alluxio.jobmanager.job.persist;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import alluxio.Constants;
import alluxio.jobmanager.job.JobDefinition;
import alluxio.wire.WorkerInfo;

public class DistributedPersistDefinition
    implements JobDefinition<DistributedPersistConfig, List<Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public Map<WorkerInfo, List<Long>> selectExecutors(DistributedPersistConfig config,
      List<WorkerInfo> workerInfoList) {
    Map<WorkerInfo, List<Long>> map = Maps.newHashMap();
    map.put(workerInfoList.get(0), Lists.newArrayList(1L, 2L));
    return map;
  }

  @Override
  public void runTask(DistributedPersistConfig config, List<Long> args) {
    LOG.info("running DistributedPersist with args:" + args);
  }

  @Override
  public String getName() {
    return "DistributedPersist";
  }
}
