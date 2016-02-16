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

package alluxio.jobmanager.master;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import alluxio.AlluxioURI;
import alluxio.jobmanager.Constants;
import alluxio.jobmanager.job.JobConfig;
import alluxio.jobmanager.job.persist.DistributedPersistConfig;
import alluxio.jobmanager.master.command.CommandManager;
import alluxio.jobmanager.master.job.JobCoordinator;
import alluxio.jobmanager.master.job.JobInfo;
import alluxio.master.AbstractMaster;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.thrift.JobManagerMasterWorkerService;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.TaskInfo;
import alluxio.util.io.PathUtils;

@ThreadSafe
public final class JobManagerMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final FileSystemMaster mFileSystemMaster;
  private final BlockMaster mBlockMaster;
  /** Manage all the jobs' status */
  private final JobIdGenerator mJobIdGenerator;
  private final CommandManager mCommandManager;
  private final Map<Long, JobCoordinator> mIdToJobCoordinator;
  private final Map<Long, JobInfo> mIdToJobInfo;

  public JobManagerMaster(FileSystemMaster fileSystemMaster, BlockMaster blockMaster,
      Journal journal) {
    super(journal, 2);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mBlockMaster = Preconditions.checkNotNull(blockMaster);
    mJobIdGenerator = new JobIdGenerator();
    mIdToJobInfo = Maps.newHashMap();
    mIdToJobCoordinator = Maps.newHashMap();
    mCommandManager = CommandManager.ISNTANCE;
  }

  private void test() {
    createJob(new DistributedPersistConfig(new AlluxioURI("test"), "underfs"));
  }

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.JOB_MANAGER_MASTER_NAME);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = Maps.newHashMap();
    services.put(Constants.JOB_MANAGER_MASTER_WORKER_SERVICE_NAME,
        new JobManagerMasterWorkerService.Processor<>(
            new JobManagerMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.JOB_MANAGER_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // do nothing
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    // do nothing
  }

  public long createJob(JobConfig jobConfig) {
    long jobId = mJobIdGenerator.getNewJobId();
    // TODO(yupeng) find job name
    JobInfo jobInfo = new JobInfo(jobId, "test", jobConfig);
    mIdToJobInfo.put(jobId, jobInfo);
    JobCoordinator jobCoordinator = JobCoordinator.create(jobInfo, mBlockMaster);
    mIdToJobCoordinator.put(jobId, jobCoordinator);
    return jobId;
  }

  public void cancelJob(long jobId) {
    // TODO validation
    JobCoordinator jobCoordinator = mIdToJobCoordinator.get(jobId);
    jobCoordinator.cancel();
  }

  public synchronized List<JobManangerCommand> workerHeartbeat(long workerId,
      List<TaskInfo> taskInfoList) {
    if (mIdToJobCoordinator.isEmpty()) {
      test();
    }
    if(!mIdToJobCoordinator.isEmpty()) {
      mIdToJobCoordinator.entrySet().iterator().next().getValue().cancel();
    }
    LOG.info(taskInfoList.toString());
    List<JobManangerCommand> comands = mCommandManager.pollAllPendingCommands(workerId);
    return comands;
  }
}
