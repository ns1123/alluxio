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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.master.policy.action.ActionDefinition;
import alluxio.master.policy.action.SchedulableAction;
import alluxio.master.policy.cond.Condition;
import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.PolicyDefinition;
import alluxio.master.policy.meta.PolicyEvaluator;
import alluxio.master.policy.meta.PolicyScope;
import alluxio.master.policy.meta.PolicyStore;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.master.policy.meta.interval.IntervalSet;
import alluxio.util.CommonUtils;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PolicyStore.class})
public final class PolicyEvaluatorTest {
  private PolicyEvaluator mEvaluator;
  private PolicyStore mStore;

  @Before
  public final void before() {
    mStore = PowerMockito.mock(PolicyStore.class);
    mEvaluator = new PolicyEvaluator(mStore);
  }

  @Test
  public void getActionsMatch() {
    long ts = CommonUtils.getCurrentMs();
    String path = "/test";
    InodeState state = mock(InodeState.class);
    when(state.isFile()).thenReturn(true);
    when(state.getId()).thenReturn(ts);
    when(state.getName()).thenReturn("test");
    Condition cond = mock(Condition.class);
    when(cond.evaluate(any(Interval.class), eq(path), any(InodeState.class)))
        .thenReturn(IntervalSet.ALWAYS);
    ActionDefinition action = mock(ActionDefinition.class);
    PolicyDefinition policy = new PolicyDefinition(ts, "ufsMigrate-" + path,
        path, PolicyScope.RECURSIVE, cond, action, ts);
    Interval interval = Interval.between(ts, ts + 1);
    when(mStore.getPolicies()).thenReturn(ImmutableList.of(policy).iterator());

    List<SchedulableAction> actions = mEvaluator.getActions(interval, path, state);

    assertEquals(1, actions.size());
    assertEquals(action, actions.get(0).getActionDefinition());
    assertEquals(PolicyKey.fromDefinition(policy), actions.get(0).getPolicyKey());
    assertEquals(IntervalSet.ALWAYS, actions.get(0).getIntervalSet());
  }

  @Test
  public void getActionsMismatchedPath() {
    long ts = CommonUtils.getCurrentMs();
    String path = "/test";
    InodeState state = mock(InodeState.class);
    when(state.isFile()).thenReturn(true);
    when(state.getId()).thenReturn(ts);
    when(state.getName()).thenReturn("test");
    Condition cond = mock(Condition.class);
    when(cond.evaluate(any(Interval.class), eq(path), any(InodeState.class)))
        .thenReturn(IntervalSet.ALWAYS);

    ActionDefinition action = mock(ActionDefinition.class);
    PolicyDefinition policy = new PolicyDefinition(ts, "ufsMigrate-" + path,
        path, PolicyScope.RECURSIVE, cond, action, ts);
    Interval interval = Interval.between(ts, ts + 1);
    when(mStore.getPolicies()).thenReturn(ImmutableList.of(policy).iterator());

    List<SchedulableAction> actions = mEvaluator.getActions(interval, "/invalid", state);

    assertEquals(0, actions.size());
  }

  @Test
  public void getActionsMismatchedCondition() {
    long ts = CommonUtils.getCurrentMs();
    String path = "/test";
    InodeState state = mock(InodeState.class);
    when(state.isFile()).thenReturn(true);
    when(state.getId()).thenReturn(ts);
    when(state.getName()).thenReturn("test");
    Condition cond = mock(Condition.class);
    when(cond.evaluate(any(Interval.class), eq(path), any(InodeState.class)))
        .thenReturn(IntervalSet.NEVER);

    ActionDefinition action = mock(ActionDefinition.class);
    PolicyDefinition policy = new PolicyDefinition(ts, "ufsMigrate-" + path,
        path, PolicyScope.RECURSIVE, cond, action, ts);
    Interval interval = Interval.between(ts, ts + 1);
    when(mStore.getPolicies()).thenReturn(ImmutableList.of(policy).iterator());

    List<SchedulableAction> actions = mEvaluator.getActions(interval, path, state);

    assertEquals(0, actions.size());
  }

  @Test
  public void isActionReady() {
    long ts = CommonUtils.getCurrentMs();
    String path = "/test";
    InodeState state = mock(InodeState.class);
    when(state.isFile()).thenReturn(true);
    when(state.getId()).thenReturn(ts);
    when(state.getName()).thenReturn("test");
    Condition cond = mock(Condition.class);
    when(cond.evaluate(any(Interval.class), eq(path), any(InodeState.class)))
        .thenReturn(IntervalSet.ALWAYS);

    ActionDefinition action = mock(ActionDefinition.class);
    PolicyDefinition policy = new PolicyDefinition(ts, "ufsMigrate-" + path,
        path, PolicyScope.RECURSIVE, cond, action, ts);
    PolicyKey key = PolicyKey.fromDefinition(policy);
    when(mStore.getPolicies()).thenReturn(ImmutableList.of(policy).iterator());
    when(mStore.getPolicy(eq(key))).thenReturn(policy);
    SchedulableAction schedulableAction = new SchedulableAction(IntervalSet.ALWAYS, action, key);

    assertTrue(mEvaluator.isActionReady(path, state, schedulableAction));
  }

  @Test
  public void isActionReadyPolicyNotFound() {
    long ts = CommonUtils.getCurrentMs();
    String path = "/test";
    InodeState state = mock(InodeState.class);
    when(state.isFile()).thenReturn(true);
    when(state.getId()).thenReturn(ts);
    when(state.getName()).thenReturn("test");
    Condition cond = mock(Condition.class);
    when(cond.evaluate(any(Interval.class), eq(path), any(InodeState.class)))
        .thenReturn(IntervalSet.ALWAYS);

    ActionDefinition action = mock(ActionDefinition.class);
    PolicyDefinition policy = new PolicyDefinition(ts, "ufsMigrate-" + path,
        path, PolicyScope.RECURSIVE, cond, action, ts);
    PolicyKey key = PolicyKey.fromDefinition(policy);
    when(mStore.getPolicies()).thenReturn(Collections.emptyIterator());
    when(mStore.getPolicy(eq(key))).thenReturn(null);
    SchedulableAction schedulableAction = new SchedulableAction(IntervalSet.ALWAYS, action, key);

    assertFalse(mEvaluator.isActionReady(path, state, schedulableAction));
  }

  @Test
  public void isActionReadyMismatchedCondition() {
    long ts = CommonUtils.getCurrentMs();
    String path = "/test";
    InodeState state = mock(InodeState.class);
    when(state.isFile()).thenReturn(true);
    when(state.getId()).thenReturn(ts);
    when(state.getName()).thenReturn("test");
    Condition cond = mock(Condition.class);
    when(cond.evaluate(any(Interval.class), eq(path), any(InodeState.class)))
        .thenReturn(IntervalSet.NEVER);

    ActionDefinition action = mock(ActionDefinition.class);
    PolicyDefinition policy = new PolicyDefinition(ts, "ufsMigrate-" + path,
        path, PolicyScope.RECURSIVE, cond, action, ts);
    PolicyKey key = PolicyKey.fromDefinition(policy);
    when(mStore.getPolicies()).thenReturn(ImmutableList.of(policy).iterator());
    when(mStore.getPolicy(eq(key))).thenReturn(policy);
    SchedulableAction schedulableAction = new SchedulableAction(IntervalSet.ALWAYS, action, key);

    assertFalse(mEvaluator.isActionReady(path, state, schedulableAction));
  }
}
