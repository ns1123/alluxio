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

package alluxio.master.policy.cond;

import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.master.policy.meta.interval.IntervalSet;

/**
 * A logical "not" expression.
 */
public class LogicalNot implements Condition {
  private final Condition mCondition;

  /**
   * Creates a new instance.
   *
   * @param condition the condition
   */
  public LogicalNot(Condition condition) {
    mCondition = condition;
  }

  @Override
  public IntervalSet evaluate(Interval interval, String path, InodeState state) {
    return mCondition.evaluate(interval, path, state).negate().intersect(new IntervalSet(interval));
  }

  @Override
  public String serialize() {
    return "NOT(" + mCondition.serialize() + ")";
  }
}
