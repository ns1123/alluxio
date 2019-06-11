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

import alluxio.master.policy.PolicyKey;
import alluxio.master.policy.meta.interval.IntervalSet;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents an action which can be scheduled at a particular time interval set.
 */
public class SchedulableAction {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulableAction.class);

  /** An interval set for which the action can be scheduled. */
  private final IntervalSet mIntervalSet;
  /** The action definition that can be scheduled. */
  private final ActionDefinition mActionDefinition;
  /** The key to the policy associated to the action. */
  private final PolicyKey mPolicyKey;

  /**
   * Creates a new instance of {@link SchedulableAction}.
   *
   * @param intervalSet the interval set for when the action can be scheduled
   * @param actionDefinition the action definition
   * @param policyKey the policy key
   */
  public SchedulableAction(IntervalSet intervalSet, ActionDefinition actionDefinition, PolicyKey policyKey) {
    Preconditions.checkState(intervalSet.isValid(), "interval set should be valid");
    mIntervalSet = intervalSet;
    mActionDefinition = actionDefinition;
    mPolicyKey = policyKey;
  }

  /**
   * @return the interval set for when the action can be scheduled
   */
  public IntervalSet getIntervalSet() {
    return mIntervalSet;
  }

  /**
   * @return the action definition
   */
  public ActionDefinition getActionDefinition() {
    return mActionDefinition;
  }

  /**
   * @return the policy key
   */
  public PolicyKey getPolicyKey() {
    return mPolicyKey;
  }
}
