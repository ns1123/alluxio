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

package alluxio.master.jobmanager.job;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import alluxio.Constants;
import alluxio.jobmanager.job.JobConfig;
import alluxio.thrift.Status;
import alluxio.thrift.TaskInfo;

/**
 * The job information used by the job manager master internally.
 */
@ThreadSafe
public final class JobInfo {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final long mId;
  private final String mName;
  private final JobConfig mJobConfig;
  private final Map<Integer, TaskInfo> mTaskIdToInfo;
  private String mErrorMessage;

  /**
   * Creates a new stance of {@link JobInfo}.
   *
   * @param id the job id
   * @param name the name of the job
   * @param jobConfig the configuration
   */
  public JobInfo(long id, String name, JobConfig jobConfig) {
    mId = id;
    mName = Preconditions.checkNotNull(name);
    // mCreationTimeMs = System.currentTimeMillis();
    mJobConfig = Preconditions.checkNotNull(jobConfig);
    mTaskIdToInfo = Maps.newHashMap();
    mErrorMessage = null;
  }

  /**
   * Registers a task.
   *
   * @param taskId the task id
   */
  public void addTask(int taskId) {
    Preconditions.checkArgument(!mTaskIdToInfo.containsKey(taskId), "");
    mTaskIdToInfo.put(taskId, new TaskInfo(mId, taskId, Status.CREATED, ""));
  }

  /**
   * @return the job id
   */
  public synchronized long getId() {
    return mId;
  }

  /**
   * @return the job name
   */
  public synchronized String getName() {
    return mName;
  }

  /**
   * @return the job configuration
   */
  public synchronized JobConfig getJobConfig() {
    return mJobConfig;
  }

  /**
   * @param errorMessage the error message
   */
  public synchronized void setErrorMessage(String errorMessage) {
    mErrorMessage = errorMessage;
  }

  /**
   * @return the error message
   */
  public synchronized String getErrorMessage() {
    return mErrorMessage;
  }

  /**
   * Sets the information of a task
   *
   * @param taskId the task id
   * @param taskInfo the task information
   */
  public synchronized void setTaskInfo(int taskId, TaskInfo taskInfo) {
    mTaskIdToInfo.put(taskId, taskInfo);
  }

  /**
   * @return the list of task ids.
   */
  public synchronized List<Integer> getTaskIdList() {
    return Lists.newArrayList(mTaskIdToInfo.keySet());
  }

  /**
   * @return the list of task information
   */
  public synchronized List<TaskInfo> getTaskInfoList() {
    return Lists.newArrayList(mTaskIdToInfo.values());
  }
}
