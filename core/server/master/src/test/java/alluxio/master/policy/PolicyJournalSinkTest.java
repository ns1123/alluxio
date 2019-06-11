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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.policy.action.ActionDefinition;
import alluxio.master.policy.action.ActionExecution;
import alluxio.master.policy.action.ActionExecutionContext;
import alluxio.master.policy.action.ActionScheduler;
import alluxio.master.policy.action.SchedulableAction;
import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.PolicyEvaluator;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.master.policy.meta.interval.IntervalSet;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PolicyEvaluator.class, ActionScheduler.class})
public final class PolicyJournalSinkTest {
  PolicyEvaluator mEvaluator;
  ActionScheduler mScheduler;
  PolicyJournalSink mSink;

  @Before
  public final void before() {
    mEvaluator = PowerMockito.mock(PolicyEvaluator.class);
    mScheduler = PowerMockito.mock(ActionScheduler.class);
    mSink = new PolicyJournalSink(mEvaluator, mScheduler);
  }

  @Test
  public void appendNewFile() {
    ArgumentCaptor<Interval> argInterval = ArgumentCaptor.forClass(Interval.class);
    ArgumentCaptor<String> argPath = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> argId = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<InodeState> argInodeState = ArgumentCaptor.forClass(InodeState.class);
    ArgumentCaptor<List<SchedulableAction>> argActions = ArgumentCaptor.forClass((Class) List.class);
    List<SchedulableAction> actions = ImmutableList.of(
        new SchedulableAction(IntervalSet.ALWAYS, new TestActionDefinition(), new PolicyKey(0, ""))
    );
    when(mEvaluator.getActions(any(Interval.class), anyString(), any(InodeState.class)))
        .thenReturn(actions);

    File.InodeFileEntry entry = File.InodeFileEntry.newBuilder()
        .setId(1234)
        .setName("testFile")
        .setPath("/testDir/testFile")
        .setPersistenceState(PersistenceState.PERSISTED.name())
        .setCreationTimeMs(System.currentTimeMillis() - 1234L)
        .setLastModificationTimeMs(System.currentTimeMillis())
        .putXAttr("testAttr", ByteString.copyFrom("testVal".getBytes()))
        .build();

    mSink.append(Journal.JournalEntry.newBuilder().setInodeFile(entry).build());
    verify(mEvaluator, after(Constants.SECOND_MS).never())
        .getActions(any(Interval.class), anyString(), any(InodeState.class));

    mSink.append(Journal.JournalEntry.newBuilder().setUpdateInodeFile(
        File.UpdateInodeFileEntry.newBuilder().setId(entry.getId()).setCompleted(true).build()
    ).build());
    verify(mEvaluator, timeout(Constants.SECOND_MS)).getActions(
        argInterval.capture(), argPath.capture(), argInodeState.capture());
    assertTrue(argInterval.getValue().getStartMs() <= System.currentTimeMillis());
    assertTrue(argInterval.getValue().getEndMs() > System.currentTimeMillis());
    assertEquals(argPath.getValue(), entry.getPath());
    assertEquals(argInodeState.getValue().getId(), entry.getId());
    assertEquals(argInodeState.getValue().getCreationTimeMs(), entry.getCreationTimeMs());
    assertEquals(
        argInodeState.getValue().getLastModificationTimeMs(), entry.getLastModificationTimeMs());
    assertEquals(argInodeState.getValue().getName(), entry.getName());
    assertEquals(
        CommonUtils.convertToByteString(argInodeState.getValue().getXAttr()), entry.getXAttrMap());

    verify(mScheduler, timeout(Constants.SECOND_MS)).scheduleActions(
        argPath.capture(), argId.capture(), argActions.capture());
    assertEquals(argPath.getValue(), entry.getPath());
    assertEquals(argId.getValue(), (Long) entry.getId());
    assertEquals(argActions.getValue(), actions);
  }

  @Test
  public void appendDeleteFile() {
    ArgumentCaptor<String> argPath = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> argId = ArgumentCaptor.forClass(Long.class);
    File.DeleteFileEntry entry = File.DeleteFileEntry.newBuilder()
        .setId(1234)
        .setPath("/testDir/testFile")
        .build();

    mSink.append(Journal.JournalEntry.newBuilder().setDeleteFile(entry).build());
    verify(mScheduler, timeout(Constants.SECOND_MS)).removeActions(
        argPath.capture(), argId.capture());
    assertEquals(argPath.getValue(), entry.getPath());
    assertEquals(argId.getValue(), (Long) entry.getId());
  }

  @Test
  public void appendRenameFile() {
    ArgumentCaptor<String> argPath = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> argId = ArgumentCaptor.forClass(Long.class);
    File.RenameEntry entry = File.RenameEntry.newBuilder()
        .setId(1234)
        .setNewName("newFile")
        .setPath("/testDir/testFile")
        .setNewPath("/testDir/newFile")
        .build();

    mSink.append(Journal.JournalEntry.newBuilder().setRename(entry).build());
    verify(mScheduler, timeout(Constants.SECOND_MS)).removeActions(
        argPath.capture(), argId.capture());
    assertEquals(argPath.getValue(), entry.getPath());
    assertEquals(argId.getValue(), (Long) entry.getId());
  }

  static class TestActionDefinition implements ActionDefinition {
    @Override
    public ActionExecution createExecution(ActionExecutionContext ctx, String path,
        InodeState inodeState) {
      return null;
    }

    @Override
    public String serialize() {
      return null;
    }
  }
}
