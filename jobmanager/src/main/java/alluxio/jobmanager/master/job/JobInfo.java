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

package alluxio.jobmanager.master.job;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import alluxio.jobmanager.job.JobConfig;
import alluxio.thrift.Status;

@ThreadSafe
// TODO(yupeng) add thread check
public class JobInfo {
  private final long mId;
  private final String mName;
  private long mCreationTimeMs;
  private final JobConfig mJobConfig;
  private final Map<Integer, Status> mTaskIdToStatus;

  public JobInfo(long id, String name, JobConfig jobConfig) {
    mId = id;
    mName = Preconditions.checkNotNull(name);
    mCreationTimeMs = System.currentTimeMillis();
    mJobConfig = Preconditions.checkNotNull(jobConfig);
    mTaskIdToStatus = Maps.newHashMap();
  }

  public void addTask(int taskId) {
    // TODO(yupeng) better exception handling
    mTaskIdToStatus.put(taskId, Status.CREATED);
  }

  public long getId() {
    return mId;
  }

  public String getName() {
    return mName;
  }

  public JobConfig getJobConfig() {
    return mJobConfig;
  }

  public List<Integer> getTaskIdList() {
    return Lists.newArrayList(mTaskIdToStatus.keySet());
  }
}
