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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.Constants;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.policy.PolicyKey;
import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.PolicyEvaluator;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.master.policy.meta.interval.IntervalSet;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ControllableScheduler;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PolicyEvaluator.class, ActionScheduler.class})
public class ActionSchedulerTest {
  @Rule
  public ManuallyScheduleHeartbeat mSchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_POLICY_ACTION_SCHEDULER);

  private PolicyEvaluator mEvaluator;
  private ControllableScheduler mActionExecutor;
  private FileSystemMaster mFileSystemMaster;
  private ActionScheduler mActionScheduler;
  private ExecutorService mHeartbeatExecutor;

  @Before
  public final void before() {
    mEvaluator = PowerMockito.mock(PolicyEvaluator.class);
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mActionExecutor = new ControllableScheduler();
    mHeartbeatExecutor =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.build("action-scheduler-test", true));
    mActionScheduler = new ActionScheduler(mEvaluator, mFileSystemMaster,
        new MasterContext(new NoopJournalSystem()), mActionExecutor);
    mActionScheduler.start(true, mHeartbeatExecutor);
  }

  @After
  public final void after() {
    mActionScheduler.stop();
  }

  @Test
  public void scheduleActionsReady() throws Exception {
    TestActionDefinition action = new TestActionDefinition();
    List<SchedulableAction> actions = ImmutableList.of(
        new SchedulableAction(IntervalSet.ALWAYS, action, new PolicyKey(0, ""))
    );
    int inodeId = 1;
    String path = "/a/b";
    when(mFileSystemMaster.getFileInfo(inodeId)).thenReturn(
        new FileInfo()
        .setFileId(inodeId)
        .setPath(path)
    );
    when(mEvaluator.isActionReady(eq(path), any(InodeState.class), any(SchedulableAction.class)))
        .thenReturn(true);

    mActionScheduler.scheduleActions(path, inodeId, actions);

    assertEquals(0, action.getActionExecutions().size());

    mActionExecutor.jumpAndExecute(1L, TimeUnit.SECONDS);

    assertEquals(1, action.getActionExecutions().size());
    verify(action.getActionExecutions().get(0)).start();

    HeartbeatScheduler.execute(HeartbeatContext.MASTER_POLICY_ACTION_SCHEDULER);
    assertEquals(1, action.getActionExecutions().size());
    verify(action.getActionExecutions().get(0)).update();
  }

  @Test
  public void scheduleActionsNotReady() throws Exception {
    TestActionDefinition action = new TestActionDefinition();
    List<SchedulableAction> actions = ImmutableList.of(
        new SchedulableAction(IntervalSet.ALWAYS, action, new PolicyKey(0, ""))
    );
    int inodeId = 1;
    String path = "/a/b";
    when(mFileSystemMaster.getFileInfo(inodeId)).thenReturn(
        new FileInfo()
            .setFileId(inodeId)
            .setPath(path)
    );
    when(mEvaluator.isActionReady(eq(path), any(InodeState.class), any(SchedulableAction.class)))
        .thenReturn(false);

    mActionScheduler.scheduleActions(path, inodeId, actions);

    assertEquals(0, action.getActionExecutions().size());

    mActionExecutor.jumpAndExecute(1L, TimeUnit.SECONDS);

    assertEquals(0, action.getActionExecutions().size());
  }

  @Test
  public void scheduleActionsInvalidInterval() throws Exception {
    TestActionDefinition action = new TestActionDefinition();
    List<SchedulableAction> actions = ImmutableList.of(
        new SchedulableAction(new IntervalSet(Interval.between(0, 1)), action, new PolicyKey(0, ""))
    );
    int inodeId = 1;
    String path = "/a/b";
    when(mFileSystemMaster.getFileInfo(inodeId)).thenReturn(
        new FileInfo()
            .setFileId(inodeId)
            .setPath(path)
    );
    when(mEvaluator.isActionReady(eq(path), any(InodeState.class), any(SchedulableAction.class)))
        .thenReturn(true);

    mActionScheduler.scheduleActions(path, inodeId, actions);

    assertEquals(0, action.getActionExecutions().size());

    mActionExecutor.jumpAndExecute(1L, TimeUnit.SECONDS);

    assertEquals(0, action.getActionExecutions().size());
  }

  @Test
  public void scheduleActionsMultipleIntervals() throws Exception {
    TestActionDefinition action = new TestActionDefinition();
    List<SchedulableAction> actions = ImmutableList.of(
        new SchedulableAction(new IntervalSet(
            ImmutableList.of(
                Interval.after(System.currentTimeMillis()),
                Interval.after(System.currentTimeMillis() + 2 * Constants.SECOND_MS),
                Interval.after(System.currentTimeMillis() + 10 * Constants.SECOND_MS)
            )), action, new PolicyKey(0, ""))
    );
    int inodeId = 1;
    String path = "/a/b";
    when(mFileSystemMaster.getFileInfo(inodeId)).thenReturn(
        new FileInfo()
            .setFileId(inodeId)
            .setPath(path)
    );
    when(mEvaluator.isActionReady(eq(path), any(InodeState.class), any(SchedulableAction.class)))
        .thenReturn(true);

    mActionScheduler.scheduleActions(path, inodeId, actions);

    assertEquals(0, action.getActionExecutions().size());

    mActionExecutor.jumpAndExecute(1L, TimeUnit.SECONDS);

    assertEquals(1, action.getActionExecutions().size());
    verify(action.getActionExecutions().get(0)).start();

    mActionExecutor.jumpAndExecute(2L, TimeUnit.SECONDS);

    assertEquals(2, action.getActionExecutions().size());
    verify(action.getActionExecutions().get(1)).start();

    mActionExecutor.jumpAndExecute(10L, TimeUnit.SECONDS);

    assertEquals(3, action.getActionExecutions().size());
    verify(action.getActionExecutions().get(2)).start();

    HeartbeatScheduler.execute(HeartbeatContext.MASTER_POLICY_ACTION_SCHEDULER);
    for (ActionExecution task : action.getActionExecutions()) {
      verify(action.getActionExecutions().get(0)).update();
    }
  }

  @Test
  public void scheduleActionsInvalidInode() throws Exception {
    TestActionDefinition action = new TestActionDefinition();
    List<SchedulableAction> actions = ImmutableList.of(
        new SchedulableAction(IntervalSet.ALWAYS, action, new PolicyKey(0, ""))
    );
    int inodeId = 1;
    String path = "/a/b";
    when(mFileSystemMaster.getFileInfo(inodeId)).thenReturn(
        new FileInfo()
            .setFileId(inodeId)
            .setPath(path)
    );
    when(mEvaluator.isActionReady(eq(path), any(InodeState.class), any(SchedulableAction.class)))
        .thenReturn(true);

    mActionScheduler.scheduleActions(path, 3, actions);

    assertEquals(0, action.getActionExecutions().size());

    mActionExecutor.jumpAndExecute(1L, TimeUnit.SECONDS);

    assertEquals(0, action.getActionExecutions().size());
  }

  @Test
  public void scheduleActionsPathMismatch() throws Exception {
    TestActionDefinition action = new TestActionDefinition();
    List<SchedulableAction> actions = ImmutableList.of(
        new SchedulableAction(IntervalSet.ALWAYS, action, new PolicyKey(0, ""))
    );
    int inodeId = 1;
    String path = "/a/b";
    when(mFileSystemMaster.getFileInfo(inodeId)).thenReturn(
        new FileInfo()
            .setFileId(inodeId)
            .setPath(path)
    );
    when(mEvaluator.isActionReady(eq(path), any(InodeState.class), any(SchedulableAction.class)))
        .thenReturn(true);

    mActionScheduler.scheduleActions("/a/c", inodeId, actions);

    assertEquals(0, action.getActionExecutions().size());

    mActionExecutor.jumpAndExecute(1L, TimeUnit.SECONDS);

    assertEquals(0, action.getActionExecutions().size());
  }

  @Test
  public void removeActions() throws Exception {
    TestActionDefinition action = new TestActionDefinition();
    List<SchedulableAction> actions = ImmutableList.of(
        new SchedulableAction(IntervalSet.ALWAYS, action, new PolicyKey(0, ""))
    );
    int inodeId = 1;
    String path = "/a/b";
    when(mFileSystemMaster.getFileInfo(inodeId)).thenReturn(
        new FileInfo()
            .setFileId(inodeId)
            .setPath(path)
    );
    when(mEvaluator.isActionReady(eq(path), any(InodeState.class), any(SchedulableAction.class)))
        .thenReturn(true);

    mActionScheduler.scheduleActions(path, inodeId, actions);

    assertEquals(0, action.getActionExecutions().size());

    mActionScheduler.removeActions(path, inodeId);

    mActionExecutor.jumpAndExecute(1L, TimeUnit.SECONDS);

    assertEquals(0, action.getActionExecutions().size());
  }

  private class TestActionDefinition implements ActionDefinition {
    List<ActionExecution> mActionExecutions = new ArrayList<>();

    @Override
    public ActionExecution createExecution(ActionExecutionContext ctx, String path,
        InodeState inodeState) {
      try {
        ActionExecution execution = Mockito.mock(ActionExecution.class);
        when(execution.update()).thenReturn(ActionStatus.IN_PROGRESS);
        mActionExecutions.add(execution);
        return execution;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String serialize() {
      return null;
    }

    public List<ActionExecution> getActionExecutions() {
      return mActionExecutions;
    }
  }
}
