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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.master.Master;
import alluxio.master.MasterFactory;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.ReadWriteJournal;

public class JobManagerMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return alluxio.jobmanager.Constants.JOB_MANAGER_MASTER_NAME;
  }

  @Override
  public Master create(List<? extends Master> masters, String journalDirectory) {
    if (!isEnabled()) {
      return null;
    }
    Preconditions.checkArgument(journalDirectory != null, "journal path may not be null");
    LOG.info("Creating {} ", JobManagerMaster.class.getName());

    ReadWriteJournal journal =
        new ReadWriteJournal(JobManagerMaster.getJournalDirectory(journalDirectory));

    FileSystemMaster fileSystemMaster = null;
    BlockMaster blockMaster = null;
    for (Master master : masters) {
      if (master instanceof FileSystemMaster) {
        fileSystemMaster = (FileSystemMaster) master;
      }
      if (master instanceof BlockMaster) {
        blockMaster = (BlockMaster) master;
      }
    }

    if (fileSystemMaster == null) {
      LOG.error("Fail to create {} due to missing {}", JobManagerMaster.class.getName(),
          FileSystemMaster.class.getName());
      return null;
    } else if (blockMaster == null) {
      LOG.error("Fail to create {} due to missing {}", JobManagerMaster.class.getName(),
          BlockMaster.class.getName());
      return null;
    } else {
      LOG.info("{} is created", JobManagerMaster.class.getName());
      return new JobManagerMaster(fileSystemMaster, blockMaster, journal);
    }
  }

}
