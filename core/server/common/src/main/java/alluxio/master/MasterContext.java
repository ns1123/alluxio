/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.master.journal.JournalSystem;

import com.google.common.base.Preconditions;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Stores context information for Alluxio masters.
 */
public final class MasterContext {
  private final JournalSystem mJournalSystem;
  private final SafeModeManager mSafeModeManager;
  private final BackupManager mBackupManager;
  private final ReadWriteLock mStateLock;
  private final long mStartTimeMs;
  private final int mPort;
  // ALLUXIO CS ADD
  private final alluxio.security.authentication.DelegationTokenManager mDelegationTokenManager;
  // ALLUXIO CS END

  // ALLUXIO CS REPLACE
  //  /**
  //   * Creates a new master context.
  //   *
  //   * The stateLock is used to allow us to pause master state changes so that we can take backups of
  //   * master state. All state modifications should hold the read lock so that holding the write lock
  //   * allows a thread to pause state modifications.
  //   *
  //   * @param journalSystem the journal system to use for tracking master operations
  //   * @param safeModeManager the manager for master safe mode
  //   * @param backupManager the backup manager for performing backups
  //   * @param startTimeMs the master process start time in milliseconds
  //   * @param port the rpc port
  //   */
  //  public MasterContext(JournalSystem journalSystem, SafeModeManager safeModeManager,
  //      BackupManager backupManager, long startTimeMs, int port) {
  //    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
  //    mSafeModeManager = Preconditions.checkNotNull(safeModeManager, "safeModeManager");
  //    mBackupManager = Preconditions.checkNotNull(backupManager, "backupManager");
  //    mStateLock = new ReentrantReadWriteLock();
  //    mStartTimeMs = startTimeMs;
  //    mPort = port;
  //  }
  // ALLUXIO CS WITH
  /**
   * Creates a new master context.
   *
   * The stateLock is used to allow us to pause master state changes so that we can take backups of
   * master state. All state modifications should hold the read lock so that holding the write lock
   * allows a thread to pause state modifications.
   *
   * @param journalSystem the journal system to use for tracking master operations
   * @param safeModeManager the manager for master safe mode
   * @param backupManager the backup manager for performing backups
   * @param delegationTokenManager the manager for managing delegation token
   * @param startTimeMs the master process start time in milliseconds
   * @param port the rpc port
   */
  public MasterContext(JournalSystem journalSystem, SafeModeManager safeModeManager,
      BackupManager backupManager,
      alluxio.security.authentication.DelegationTokenManager delegationTokenManager,
      long startTimeMs, int port) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mSafeModeManager = Preconditions.checkNotNull(safeModeManager, "safeModeManager");
    mBackupManager = Preconditions.checkNotNull(backupManager, "backupManager");
    mDelegationTokenManager = Preconditions.checkNotNull(delegationTokenManager,
        "delegationTokenManager");
    mStateLock = new ReentrantReadWriteLock();
    mStartTimeMs = startTimeMs;
    mPort = port;
  }
  // ALLUXIO CS END

  /**
   * Create a master context to be used for job masters.
   *
   * @param journalSystem the journal system to use for tracking master operations
   */
  public MasterContext(JournalSystem journalSystem) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mSafeModeManager = null;
    mBackupManager = null;
    mDelegationTokenManager = null;
    mStateLock = new ReentrantReadWriteLock();
    mStartTimeMs = -1;
    mPort = -1;
  }

  /**
   * @return the journal system to use for tracking master operations
   */
  public JournalSystem getJournalSystem() {
    return mJournalSystem;
  }

  /**
   * @return the manager for master safe mode
   */
  public SafeModeManager getSafeModeManager() {
    return mSafeModeManager;
  }

  /**
   * @return the backup manager
   */
  public BackupManager getBackupManager() {
    return mBackupManager;
  }
  // ALLUXIO CS ADD

  /**
   * @return the manager for delegation tokens
   */
  public alluxio.security.authentication.DelegationTokenManager getDelegationTokenManager() {
    return mDelegationTokenManager;
  }
  // ALLUXIO CS END

  /**
   * @return the lock which must be held to modify master state
   */
  public Lock stateChangeLock() {
    return mStateLock.readLock();
  }

  /**
   * @return the lock which prevents master state from changing
   */
  public Lock pauseStateLock() {
    return mStateLock.writeLock();
  }

  /**
   * @return the master process start time in milliseconds
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the rpc port
   */
  public int getPort() {
    return mPort;
  }
}
