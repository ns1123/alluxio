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

package alluxio.master.policy.meta;

import alluxio.master.policy.PolicyKey;
import alluxio.master.policy.action.SchedulableAction;
import alluxio.master.policy.cond.Condition;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.master.policy.meta.interval.IntervalSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * This evaluates the conditions of the policies and determines which actions should be generated.
 */
public final class PolicyEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(PolicyEvaluator.class);

  private final PolicyStore mPolicyStore;

  /**
   * Creates a new instance of {@link PolicyEvaluator}.
   *
   * @param policyStore the {@link PolicyStore}
   */
  public PolicyEvaluator(PolicyStore policyStore) {
    mPolicyStore = policyStore;
  }

  /**
   * @param interval the time interval to consider
   * @param path the Alluxio path to retrieve actions for
   * @param state the state of the path
   * @return the list of schedulable actions applicable for the given path and state
   */
  public List<SchedulableAction> getActions(Interval interval, String path, InodeState state) {
    if (!state.isFile()) {
      return Collections.emptyList();
    }
    List<SchedulableAction> actions = Collections.emptyList();
    Iterator<PolicyDefinition> it = mPolicyStore.getPolicies();
    while (it.hasNext()) {
      PolicyDefinition policy = it.next();
      IntervalSet intervalSet = evaluatePolicy(path, state, policy, interval);
      if (intervalSet.isValid()) {
        if (actions.isEmpty()) {
          // lazily create the list
          actions = new ArrayList<>(2);
        }
        actions.add(new SchedulableAction(intervalSet, policy.getAction(),
            PolicyKey.fromDefinition(policy)));
      }
    }
    return actions;
  }

  /**
   * @param path the Alluxio path to retrieve actions for
   * @param state the state of the path
   * @param action the action to be checked
   * @return whether the conditions for the action are all satisfied
   */
  public boolean isActionReady(String path, InodeState state, SchedulableAction action) {
    if (!state.isFile()) {
      return false;
    }
    PolicyKey policyKey = action.getPolicyKey();
    PolicyDefinition policy = mPolicyStore.getPolicy(policyKey);
    if (policy == null) {
      return false;
    }
    long currentTimeMS = System.currentTimeMillis();
    Interval interval = Interval.between(currentTimeMS, currentTimeMS + 1);
    return evaluatePolicy(path, state, policy, interval).isValid();
  }

  private IntervalSet evaluatePolicy(String path, InodeState state, PolicyDefinition policy,
      Interval interval) {
    if (policy.getScope() == PolicyScope.SINGLE) {
      if (!policy.getPath().equals(path)) {
        return IntervalSet.NEVER;
      }
    } else if (policy.getScope() == PolicyScope.RECURSIVE) {
      if (!path.startsWith(policy.getPath())) {
        return IntervalSet.NEVER;
      }
    } else if (policy.getScope() == PolicyScope.RECURSIVE_MOUNT) {
      // TODO(gpang): how to determine if in the same mount?
      return IntervalSet.NEVER;
    }
    Condition condition = policy.getCondition();
    IntervalSet intervalSet = condition.evaluate(
        interval, path, state);
    return intervalSet;
  }
}
