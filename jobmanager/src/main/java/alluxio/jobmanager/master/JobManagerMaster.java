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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import alluxio.jobmanager.Constants;
import alluxio.master.AbstractMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.thrift.JobManagerMasterWorkerService;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.RunTaskCommand;
import alluxio.thrift.TaskStatus;
import alluxio.util.io.PathUtils;

@ThreadSafe
public final class JobManagerMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final FileSystemMaster mFileSystemMaster;

  public JobManagerMaster(FileSystemMaster fileSystemMaster, Journal journal) {
    super(journal, 2);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
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

  public synchronized List<JobManangerCommand> workerHeartbeat(long workerId,
      List<TaskStatus> taskStatusList) {
    LOG.info(taskStatusList.toString());

    RunTaskCommand runTaskCommand = new RunTaskCommand();
    runTaskCommand.setJobId(1L);
    List<String> jobConfig = Lists.newArrayList("test");
    // serialize

    try {
      runTaskCommand.setJobConfig(serialize(jobConfig));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    JobManangerCommand command = new JobManangerCommand();
    command.setRunTaskCommand(runTaskCommand);
    return Lists.newArrayList(command);
  }

  public static byte[] serialize(Object obj) throws IOException {
    try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
      try (ObjectOutputStream o = new ObjectOutputStream(b)) {
        o.writeObject(obj);
      }
      return b.toByteArray();
    }
  }
}
