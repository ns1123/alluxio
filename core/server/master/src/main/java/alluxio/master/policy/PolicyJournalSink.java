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

package alluxio.master.policy;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.journal.sink.JournalSink;
import alluxio.master.policy.action.ActionScheduler;
import alluxio.master.policy.action.SchedulableAction;
import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.PolicyEvaluator;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;

/**
 * This examines all the journal entries and determines if any actions should be generated.
 */
public final class PolicyJournalSink implements JournalSink {
  private static final Logger LOG = LoggerFactory.getLogger(PolicyJournalSink.class);
  private static final int DEFAULT_POLICY_JOURNAL_PROCESSING_POOL_SIZE = 1;
  // TODO(feng): this should be defined as a configurable property used by both
  //  scanner and journal sink
  private static final long POLICY_ENGINE_SCAN_INTERVAL =
      ServerConfiguration.getMs(PropertyKey.POLICY_SCAN_INTERVAL);
  private static final long POLICY_ENGINE_ACTION_CHECK_SPAN = POLICY_ENGINE_SCAN_INTERVAL * 2;
  private static final long POLICY_ENGINE_MAX_INCOMPLETE_FILE_ENTRIES =
      ServerConfiguration.getLong(PropertyKey.POLICY_INCREMENTAL_INCOMPLETE_FILES_MAX);

  private final PolicyEvaluator mPolicyEvaluator;
  private final ActionScheduler mActionScheduler;
  private final Cache<Long, File.InodeFileEntry> mIncompleteFiles;
  private final BlockingQueue<Journal.JournalEntry> mEntriesToProcess;
  private final ExecutorService mExecutorPool;

  /**
   * Creates a new instance of {@link PolicyJournalSink}.
   *
   * @param policyEvaluator the {@link PolicyEvaluator}
   * @param actionScheduler the {@link ActionScheduler}
   */
  public PolicyJournalSink(PolicyEvaluator policyEvaluator, ActionScheduler actionScheduler) {
    mPolicyEvaluator = policyEvaluator;
    mActionScheduler = actionScheduler;
    mEntriesToProcess = new LinkedBlockingQueue<>();
    mIncompleteFiles = CacheBuilder.newBuilder()
        .maximumSize(POLICY_ENGINE_MAX_INCOMPLETE_FILE_ENTRIES).build();
    mExecutorPool = Executors.newFixedThreadPool(DEFAULT_POLICY_JOURNAL_PROCESSING_POOL_SIZE,
        ThreadFactoryUtils.build("policy-journal-sink-%d", true));
    for (int i = 0; i < DEFAULT_POLICY_JOURNAL_PROCESSING_POOL_SIZE; i++) {
      mExecutorPool.execute(new PolicyJournalEntryProcessor());
    }
  }

  @Override
  public void append(Journal.JournalEntry entry) {
    mEntriesToProcess.add(entry);
  }

  @Override
  public void flush() {
    // Nothing to do
  }

  private class PolicyJournalEntryProcessor implements Runnable {
    private String mThreadName;

    @Override
    public void run() {
      mThreadName = Thread.currentThread().getName();
      Journal.JournalEntry entry;
      while (true) {
        try {
          entry = mEntriesToProcess.take();
          processEntry(entry);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Thread {} is interrupted.", mThreadName);
        }
      }
    }

    private void processEntry(Journal.JournalEntry entry) {
      // TODO(feng): implement
      String pathCreated = null;
      String pathRemoved = null;
      InodeState inodeState = null;
      long inodeId = -1;
      if (entry.hasInodeFile()) {
        mIncompleteFiles.put(entry.getInodeFile().getId(), entry.getInodeFile());
      }
      if (entry.hasUpdateInodeFile() && entry.getUpdateInodeFile().getCompleted()) {
        File.InodeFileEntry inodeEntry = mIncompleteFiles.getIfPresent(
            entry.getUpdateInodeFile().getId());
        if (inodeEntry != null) {
          mIncompleteFiles.invalidate(inodeEntry.getId());
          pathCreated = inodeEntry.getPath();
          inodeId = inodeEntry.getId();
          inodeState = new JournalEntryInodeState(inodeEntry.getId(), inodeEntry.getName(),
              inodeEntry.getCreationTimeMs(), inodeEntry.getLastModificationTimeMs(),
              CommonUtils.convertFromByteString(inodeEntry.getXAttrMap()), false,
              PersistenceState
                  .valueOf(inodeEntry.getPersistenceState()) == PersistenceState.PERSISTED, false);
        }
      }
      if (entry.hasInodeDirectory()) {
        File.InodeDirectoryEntry inodeEntry = entry.getInodeDirectory();
        pathCreated = inodeEntry.getPath();
        inodeId = inodeEntry.getId();
        inodeState = new JournalEntryInodeState(inodeEntry.getId(), inodeEntry.getName(),
            inodeEntry.getCreationTimeMs(), inodeEntry.getLastModificationTimeMs(),
            CommonUtils.convertFromByteString(inodeEntry.getXAttrMap()), true,
            PersistenceState
                .valueOf(inodeEntry.getPersistenceState()) == PersistenceState.PERSISTED,
            inodeEntry.getMountPoint());
      }
      if (entry.hasRename()) {
        // TODO(feng): add code to handle path creation in rename event
        pathRemoved = entry.getRename().getPath();
        inodeId = entry.getRename().getId();
      }
      if (entry.hasDeleteFile()) {
        pathRemoved = entry.getDeleteFile().getPath();
        inodeId = entry.getDeleteFile().getId();
      }
      // TODO(feng): add code to handle persistence status change
      if (pathCreated != null) {
        long currentTime = System.currentTimeMillis();
        List<SchedulableAction> actions = mPolicyEvaluator.getActions(
            Interval.between(currentTime, currentTime + POLICY_ENGINE_ACTION_CHECK_SPAN),
            pathCreated, inodeState);
        mActionScheduler.scheduleActions(pathCreated, inodeId, actions);
      }
      if (pathRemoved != null) {
        mActionScheduler.removeActions(pathRemoved, inodeId);
      }
    }
  }

  private static final class JournalEntryInodeState implements InodeState {
    private final long mCreationTimeMs;
    private final long mId;
    private final long mLastModificationTimeMs;
    private final String mName;
    private final Map<String, byte[]> mXattr;
    private final boolean mDirectory;
    private final boolean mPersisted;
    private final boolean mIsMountPoint;

    public JournalEntryInodeState(long id, String name, long creationTimeMs,
        long lastModificationTimeMs, Map<String, byte[]> xattr, boolean isDirectory,
        boolean isPersisted, boolean isMountPoint) {
      mId = id;
      mName = name;
      mCreationTimeMs = creationTimeMs;
      mLastModificationTimeMs = lastModificationTimeMs;
      mXattr = xattr;
      mDirectory = isDirectory;
      mPersisted = isPersisted;
      mIsMountPoint = isMountPoint;
    }

    @Override
    public long getCreationTimeMs() {
      return mCreationTimeMs;
    }

    @Override
    public long getId() {
      return mId;
    }

    @Override
    public long getLastModificationTimeMs() {
      return mLastModificationTimeMs;
    }

    @Override
    public String getName() {
      return mName;
    }

    @Nullable
    @Override
    public Map<String, byte[]> getXAttr() {
      return mXattr;
    }

    @Override
    public boolean isDirectory() {
      return mDirectory;
    }

    @Override
    public boolean isFile() {
      return !mDirectory;
    }

    @Override
    public boolean isPersisted() {
      return mPersisted;
    }

    @Override
    public boolean isMountPoint() {
      return mIsMountPoint;
    }
  }
}
