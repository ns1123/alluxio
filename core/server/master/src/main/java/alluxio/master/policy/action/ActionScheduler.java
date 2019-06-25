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

package alluxio.master.policy.action;

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.job.JobMasterClientPool;
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.MasterContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.PolicyEvaluator;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.proto.journal.Journal;
import alluxio.util.LogUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.FileInfo;
import alluxio.worker.job.JobMasterClientContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

/**
 * This manages the scheduling and execution of actions for the policy master.
 */
public final class ActionScheduler implements Journaled {
  private static final Logger LOG = LoggerFactory.getLogger(ActionScheduler.class);
  private static final long ACTION_SCHEDULER_HEARTBEAT_INTERVAL_MS =
      ServerConfiguration.getMs(PropertyKey.POLICY_ACTION_SCHEDULER_HEARTBEAT_INTERVAL);
  private static final int ACTION_SCHEDULER_THREAD_POOL_SIZE =
      ServerConfiguration.getInt(PropertyKey.POLICY_ACTION_SCHEDULER_THREADS);
  private static final int ACTION_SCHEDULER_MAX_RUNNING_ACTIONS =
      ServerConfiguration.getInt(PropertyKey.POLICY_ACTION_SCHEDULER_RUNNING_ACTIONS_MAX);
  private static final int ACTION_EXECUTION_THREAD_POOL_SIZE =
      ServerConfiguration.getInt(PropertyKey.POLICY_ACTION_EXECUTION_THREADS);
  private static final int ACTION_COMMIT_THREAD_POOL_SIZE =
      ServerConfiguration.getInt(PropertyKey.POLICY_ACTION_COMMIT_THREADS);

  private final FileSystemMaster mFileSystemMaster;
  private final PolicyEvaluator mPolicyEvaluator;
  private final MasterContext mMasterContext;
  private final ScheduledExecutorService mActionExecutor;
  private final ConcurrentHashMap<InodeKey, List<ScheduledFuture>> mScheduledTasks;
  private final ConcurrentHashMap<InodeKey, ActionExecution> mExecutingActions;
  private final ConcurrentHashSet<ActionExecution> mCommittingActions;

  private JobMasterClientPool mJobMasterClientPool;
  private ExecutorService mActionExecutionService;
  private ExecutorService mActionCommitService;
  private ActionExecutionContext mActionExecutionContext;
  private volatile boolean mShouldExecute;

  /**
   * Creates a new instance of {@link ActionScheduler}.
   *
   * @param policyEvaluator the policy evaluator to use
   * @param masterContext the context for master
   * @param fileSystemMaster the filesystem master
   */
  public ActionScheduler(PolicyEvaluator policyEvaluator, FileSystemMaster fileSystemMaster,
      MasterContext masterContext) {
    this(policyEvaluator, fileSystemMaster, masterContext,
        new ScheduledThreadPoolExecutor(ACTION_SCHEDULER_THREAD_POOL_SIZE,
            ThreadFactoryUtils.build(ActionScheduler.class.getSimpleName() + "-%d", true)));
  }

  /**
   * Creates a new instance of {@link ActionScheduler} using the given executor.
   *
   * @param actionExecutor the executor used for scheduling actions
   * @param policyEvaluator the policy evaluator to use
   * @param fileSystemMaster the filesystem master
   * @param masterContext the context for master
   */
  public ActionScheduler(PolicyEvaluator policyEvaluator, FileSystemMaster fileSystemMaster,
      MasterContext masterContext, ScheduledExecutorService actionExecutor) {
    mActionExecutor = Preconditions.checkNotNull(actionExecutor);
    mPolicyEvaluator = Preconditions.checkNotNull(policyEvaluator);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mScheduledTasks = new ConcurrentHashMap<>();
    mExecutingActions = new ConcurrentHashMap<>();
    mCommittingActions = new ConcurrentHashSet<>();
    mMasterContext = masterContext;
  }

  @Override
  public CheckpointName getCheckpointName() {
    // TODO(gpang): implement
    return null;
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    // TODO(gpang): implement
    return false;
  }

  @Override
  public void resetState() {
    // TODO(gpang): implement
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    // TODO(gpang): implement
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    // TODO(gpang): implement
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    // TODO(gpang): implement
    return Collections.emptyIterator();
  }

  /**
   * Schedule actions to be executed for a particular path.
   *
   * @param path the path of the inode
   * @param inodeId the id of the inode
   * @param actions the actions to be scheduled
   */
  public void scheduleActions(String path, long inodeId, List<SchedulableAction> actions) {
    long currentTimeMs = System.currentTimeMillis();
    Interval afterNow = Interval.after(currentTimeMs);
    for (SchedulableAction action : actions) {
      for (Interval interval : action.getIntervalSet().getIntervals()) {
        // find the earliest time this action can be scheduled
        Interval schedulableInterval = interval.intersect(afterNow);
        if (schedulableInterval.isValid()) {
          scheduleAction(new InodeKey(path, inodeId), action,
              schedulableInterval.getStartMs() - currentTimeMs);
        }
      }
    }
  }

  private void scheduleAction(InodeKey inodeKey, SchedulableAction action, long delayMs) {
    ScheduledFuture<?> future = mActionExecutor.schedule(
        () -> executeAction(inodeKey, action), delayMs, TimeUnit.MILLISECONDS);
    mScheduledTasks.compute(
        inodeKey, (key, tasks) -> {
          if (tasks == null) {
            tasks = new ArrayList<>(1);
          }
          tasks.add(future);
          return tasks;
        });
  }

  /**
   * Remove scheduled actions for a particular path and all its children.
   *
   * @param path the path of the inode
   * @param inodeId the id of the inode
   */
  public void removeActions(String path, long inodeId) {
    List<ScheduledFuture> tasks = mScheduledTasks.remove(new InodeKey(path, inodeId));
    if (tasks != null) {
      tasks.forEach(task -> task.cancel(false));
    }
  }

  private void executeAction(InodeKey inodeKey, SchedulableAction action) {
    if (!mShouldExecute) {
      // TODO(feng): we might miss some actions during leadership transition, which will be
      //  rescheduled by fixed interval policy scanner. Consider reschedule execution when
      //  mShouldExecute is set to true for better timing guarantee.
      return;
    }
    if (mExecutingActions.size() > ACTION_SCHEDULER_MAX_RUNNING_ACTIONS) {
      // TODO(feng): this will wake up the scheduler thread pool frequently, consider
      //  adding a queue to offload the reschedule work to heartbeat thread
      scheduleAction(inodeKey, action, Constants.MINUTE_MS);
      return;
    }
    String path = inodeKey.getPath();
    FileInfo file;
    try {
      file = mFileSystemMaster.getFileInfo(inodeKey.getInodeId());
    } catch (AccessControlException | IOException | FileDoesNotExistException e) {
      LOG.debug("Unable to get file info for path {} while executing action: {}", path,
          e.getMessage());
      return;
    }
    if (!inodeKey.getPath().equals(file.getPath())) {
      LOG.debug("Unable to execute action for {} because file is renamed.", path);
      return;
    }
    InodeState inodeState = new FileInfoInodeState(file);
    if (mPolicyEvaluator.isActionReady(path, inodeState, action)) {
      AtomicBoolean createdNewExecution = new AtomicBoolean(false);
      ActionExecution execution = mExecutingActions.computeIfAbsent(inodeKey, (key) -> {
        createdNewExecution.set(true);
        return action.getActionDefinition()
            .createExecution(mActionExecutionContext, path, inodeState);
      });
      if (!createdNewExecution.get()) {
        scheduleAction(inodeKey, action, Constants.MINUTE_MS);
        return;
      }
      if (execution.start() == ActionStatus.FAILED) {
        // action is removed by heartbeat, so rely on that to avoid race condition
        String actionDescription = action.getActionDefinition().serialize();
        LOG.error(String.format("Failed to start action %s for path %s",
            actionDescription, path), execution.getException());
        try {
          execution.close();
        } catch (IOException e) {
          LOG.error(String.format("Failed to close action %s for path %s",
              actionDescription, path), e);
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip executing action {} for path {}: policy conditions no longer hold.",
            action.getActionDefinition().serialize(), path);
      }
    }
  }

  /**
   * Starts the scheduler.
   *
   * @param shouldExecute whether the scheduled actions should be executed
   * @param heartbeatExecutor the executor for heartbeat thread
   */
  public void start(boolean shouldExecute, ExecutorService heartbeatExecutor) {
    mShouldExecute = shouldExecute;
    if (!shouldExecute) {
      return;
    }
    heartbeatExecutor.submit(new HeartbeatThread(
        HeartbeatContext.MASTER_POLICY_ACTION_SCHEDULER,
        new ActionSchedulerHeartbeatExecutor(),
        ACTION_SCHEDULER_HEARTBEAT_INTERVAL_MS,
        ServerConfiguration.global(), mMasterContext.getUserState()));
    mJobMasterClientPool =
        new JobMasterClientPool(JobMasterClientContext.newBuilder(ClientContext.create(
            ServerConfiguration.global())).build());
    mActionExecutionService = Executors.newFixedThreadPool(
        ACTION_EXECUTION_THREAD_POOL_SIZE,
        ThreadFactoryUtils.build("action-execution-service-%d", true));
    mActionCommitService = Executors.newFixedThreadPool(
        ACTION_COMMIT_THREAD_POOL_SIZE,
        ThreadFactoryUtils.build("action-commit-service-%d", true));
    mActionExecutionContext = new ActionExecutionContext(mFileSystemMaster, mActionExecutionService,
        mJobMasterClientPool);
  }

  /**
   * Stops the scheduler.
   */
  public void stop() {
    if (!mShouldExecute) {
      return;
    }
    // heartbeat thread is stopped by the owner of the heartbeatExecutor
    mActionExecutionService.shutdown();
    mActionCommitService.shutdown();

    try {
      mJobMasterClientPool.close();
    } catch (IOException e) {
      LOG.warn("Failed to close job master client pool while shutting down.", e);
    }
    mShouldExecute = false;
  }

  /**
   * @return the number of actions being executed
   */
  @VisibleForTesting
  public int getExecutingActionsSize() {
    return mExecutingActions.size();
  }

  private class FileInfoInodeState implements InodeState {
    private final FileInfo mFileInfo;

    public FileInfoInodeState(FileInfo file) {
      mFileInfo = file;
    }

    @Override
    public long getCreationTimeMs() {
      return mFileInfo.getCreationTimeMs();
    }

    @Override
    public long getId() {
      return mFileInfo.getFileId();
    }

    @Override
    public long getLastModificationTimeMs() {
      return mFileInfo.getLastModificationTimeMs();
    }

    @Override
    public String getName() {
      return mFileInfo.getName();
    }

    @Nullable
    @Override
    public Map<String, byte[]> getXAttr() {
      return mFileInfo.getXAttr();
    }

    @Override
    public boolean isDirectory() {
      return mFileInfo.isFolder();
    }

    @Override
    public boolean isFile() {
      return !mFileInfo.isFolder();
    }

    @Override
    public boolean isPersisted() {
      return mFileInfo.isPersisted();
    }

    @Override
    public boolean isMountPoint() {
      return mFileInfo.isMountPoint();
    }
  }

  private class InodeKey {
    private final String mPath;
    private final long mInodeId;

    InodeKey(String path, long inodeId) {
      mPath = path;
      mInodeId = inodeId;
    }

    public String getPath() {
      return mPath;
    }

    public long getInodeId() {
      return mInodeId;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mPath, mInodeId);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof InodeKey)) {
        return false;
      }
      return this.mInodeId == ((InodeKey) o).mInodeId
          && Objects.equal(this.mPath, ((InodeKey) o).mPath);
    }
  }

  private class ActionSchedulerHeartbeatExecutor implements HeartbeatExecutor {

    @Override
    public void heartbeat() {
      updateExecutingActions();
      cleanupScheduledActions();
    }

    private void updateExecutingActions() {
      for (Iterator<ActionExecution> iterator = mExecutingActions.values().iterator(); iterator.hasNext();) {
        ActionExecution action = iterator.next();
        ActionStatus status;
        try {
          status = action.update();
        } catch (IOException e) {
          LOG.warn("Failed to update status for action {}: {}", action, e);
          continue;
        }
        switch (status) {
          case PREPARED:
            if (mCommittingActions.addIfAbsent(action)) {
              LOG.debug("Action {} is prepared, committing it asynchronously", action);
              mActionCommitService.submit(action::commit);
            }
            break;
          case COMMITTED:
            LOG.debug("Action {} is committed", action);
            mCommittingActions.remove(action);
            iterator.remove();
            break;
          case FAILED:
            LOG.error("Failed to execute action {}: {}", action, action.getException());
            try {
              action.close();
            } catch (IOException e) {
              LOG.error("Failed to close action {}: {}", action, e);
            }
            mCommittingActions.remove(action);
            iterator.remove();
            break;
          default:
            break;
        }
      }
    }

    private void cleanupScheduledActions() {
      for (Iterator<InodeKey> iterator
           = mScheduledTasks.keySet().iterator(); iterator.hasNext();) {
        List<ScheduledFuture> newValue = mScheduledTasks.computeIfPresent(iterator.next(),
            (key, tasks) -> {
              tasks.removeIf(task -> task.isDone() || task.isCancelled());
              return tasks;
            });
        if (newValue != null && newValue.isEmpty()) {
          iterator.remove();
        }
      }
    }

    @Override
    public void close() {
      mExecutingActions.values().forEach((action) -> {
        try {
          action.close();
        } catch (IOException e) {
          LogUtils.warnWithException(LOG,
              "Failed to stop action {} while shutting down.", action, e);
        }
      });
      mExecutingActions.clear();
    }
  }
}
