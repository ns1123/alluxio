/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.Constants;
import alluxio.master.Master;
import alluxio.master.MasterFactory;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.ReadWriteJournal;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link JobMaster} instance.
 */
@ThreadSafe
public final class JobMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return Constants.JOB_MASTER_NAME;
  }

  @Override
  public Master create(List<? extends Master> masters, String journalDirectory) {
    if (!isEnabled()) {
      return null;
    }
    Preconditions.checkArgument(journalDirectory != null, "journal path may not be null");
    LOG.info("Creating {} ", JobMaster.class.getName());

    ReadWriteJournal journal =
        new ReadWriteJournal(JobMaster.getJournalDirectory(journalDirectory));

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
      LOG.error("Fail to create {} due to missing {}", JobMaster.class.getName(),
          FileSystemMaster.class.getName());
      return null;
    } else if (blockMaster == null) {
      LOG.error("Fail to create {} due to missing {}", JobMaster.class.getName(),
          BlockMaster.class.getName());
      return null;
    } else {
      LOG.info("{} is created", JobMaster.class.getName());
      return new JobMaster(fileSystemMaster, blockMaster, journal);
    }
  }

}
