/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Process;
import alluxio.PropertyKey;
import alluxio.master.job.JobMaster;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.util.URIUtils;

import com.google.common.base.Preconditions;

import java.net.InetSocketAddress;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job master in the Alluxio system.
 */
public interface JobMasterProcess extends Process {
  /**
   * Factory for creating {@link JobMasterProcess}.
   */
  @ThreadSafe
  final class Factory {

    /**
     * @return a new instance of {@link JobMasterProcess}
     */
    public static JobMasterProcess create() {
      URI journalLocation = JournalUtils.getJournalLocation();
      JournalSystem journalSystem = new JournalSystem.Builder()
          .setLocation(URIUtils.appendPathOrDie(journalLocation, Constants.JOB_JOURNAL_NAME))
          .build();
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        Preconditions.checkState(
            !(journalSystem instanceof alluxio.master.journal.raft.RaftJournalSystem),
            "Raft journal cannot be used with Zookeeper enabled");
        PrimarySelector primarySelector = PrimarySelector.Factory.createZkJobPrimarySelector();
        return new FaultTolerantAlluxioJobMasterProcess(journalSystem, primarySelector);
      } else if (journalSystem instanceof alluxio.master.journal.raft.RaftJournalSystem) {
        PrimarySelector primarySelector =
            ((alluxio.master.journal.raft.RaftJournalSystem) journalSystem)
                .getPrimarySelector();
        return new FaultTolerantAlluxioJobMasterProcess(journalSystem, primarySelector);
      }
      return new AlluxioJobMasterProcess(journalSystem);
    }

    private Factory() {} // prevent instantiation
  }

  /**
   * @return internal {@link JobMaster}
   */
  JobMaster getJobMaster();

  /**
   * @return this master's rpc address
   */
  InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the master in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the master in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the master's web address, or null if the web server hasn't been started yet
   */
  InetSocketAddress getWebAddress();

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing();
}
