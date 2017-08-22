/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalSystem.Mode;
import alluxio.util.CommonUtils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The fault tolerant version of {@link AlluxioJobMaster} that uses zookeeper and standby masters.
 */
@NotThreadSafe
final class FaultTolerantAlluxioJobMasterProcess extends AlluxioJobMasterProcess {
  private static final Logger LOG =
      LoggerFactory.getLogger(FaultTolerantAlluxioJobMasterProcess.class);

  private PrimarySelector mLeaderSelector;
  private Thread mServingThread;

  /**
   * Creates a {@link FaultTolerantAlluxioJobMasterProcess}.
   */
  protected FaultTolerantAlluxioJobMasterProcess(JournalSystem journalSystem,
      PrimarySelector leaderSelector) {
    super(journalSystem);
    try {
      stopServing();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mLeaderSelector = Preconditions.checkNotNull(leaderSelector, "leaderSelector");
    mServingThread = null;
  }

  @Override
  public void start() throws Exception {
    mJournalSystem.start();
    try {
      mLeaderSelector.start(getRpcAddress());
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }

    while (!Thread.interrupted()) {
      if (mLeaderSelector.isPrimary()) {
        if (mServingThread == null) {
          LOG.info("Transitioning from secondary to primary");
          mJournalSystem.setMode(Mode.PRIMARY);
          stopMaster();
          LOG.info("Secondary stopped");
          startMaster(true);
          mServingThread = new Thread(new Runnable() {
            @Override
            public void run() {
              startServing("(gained leadership)", "(lost leadership)");
            }
          }, "MasterServingThread");
          mServingThread.start();
          LOG.info("Primary started");
        }
      } else {
        // This master should be standby, and not the leader
        if (mServingThread != null) {
          LOG.info("Transitioning from primary to secondary");
          // Need to transition this master to standby mode.
          mServingThread.interrupt();
          mServingThread.join();
          mServingThread = null;
          stopServing();
          stopMaster();
          mJournalSystem.setMode(Mode.SECONDARY);
          LOG.info("Primary stopped");
          startMaster(false);
          LOG.info("Secondary started");
        }
        // This master is already in standby mode. No further actions needed.
      }

      CommonUtils.sleepMs(LOG, 100);
    }
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    if (mLeaderSelector != null) {
      mLeaderSelector.stop();
    }
  }

  @Override
  public void waitForReady() {
    CommonUtils.waitFor(this + " to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return (!mLeaderSelector.isPrimary() || isServing());
      }
    });
  }
}
