/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job.meta;

import alluxio.Constants;
import alluxio.job.JobConfig;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The job information used by the job master internally.
 */
@ThreadSafe
public final class JobInfo {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final long mId;
  private final String mName;
  private final JobConfig mJobConfig;
  private final Map<Integer, TaskInfo> mTaskIdToInfo;
  private String mErrorMessage;
  private Status mStatus;
  private String mResult;

  /**
   * Creates a new instance of {@link JobInfo}.
   *
   * @param id the job id
   * @param name the name of the job
   * @param jobConfig the configuration
   */
  public JobInfo(long id, String name, JobConfig jobConfig) {
    mId = id;
    mName = Preconditions.checkNotNull(name);
    mJobConfig = jobConfig;
    mTaskIdToInfo = Maps.newHashMap();
    mErrorMessage = "";
    mStatus = Status.CREATED;
  }

  /**
   * Registers a task.
   *
   * @param taskId the task id
   */
  public void addTask(int taskId) {
    Preconditions.checkArgument(!mTaskIdToInfo.containsKey(taskId), "");
    mTaskIdToInfo.put(taskId, new TaskInfo().setJobId(mId).setTaskId(taskId)
        .setStatus(Status.CREATED).setErrorMessage("").setResult(null));
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
   * @param taskId the task ID to get the task info for
   * @return the task info, or null if the task ID doesn't exist
   */
  public synchronized TaskInfo getTaskInfo(int taskId) {
    return mTaskIdToInfo.get(taskId);
  }

  /**
   * Sets the information of a task.
   *
   * @param taskId the task id
   * @param taskInfo the task information
   */
  public synchronized void setTaskInfo(int taskId, TaskInfo taskInfo) {
    mTaskIdToInfo.put(taskId, taskInfo);
  }

  /**
   * @return the list of task ids
   */
  public synchronized List<Integer> getTaskIdList() {
    return Lists.newArrayList(mTaskIdToInfo.keySet());
  }

  /**
   * @param status the job status
   */
  public void setStatus(Status status) {
    mStatus = status;
  }

  /**
   * @return the status of the job
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @param result the joined job result
   */
  public void setResult(String result) {
    mResult = result;
  }

  /**
   * @return the result of the job
   */
  public String getResult() {
    return mResult;
  }

  /**
   * @return the list of task information
   */
  public synchronized List<TaskInfo> getTaskInfoList() {
    return Lists.newArrayList(mTaskIdToInfo.values());
  }
}
