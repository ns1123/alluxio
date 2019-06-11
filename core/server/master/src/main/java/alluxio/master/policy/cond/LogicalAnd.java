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
import alluxio.master.policy.meta.interval.IntervalUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A logical "and" expression.
 */
public class LogicalAnd implements Condition {
  private final List<Condition> mConditions;

  /**
   * Creates a new instance.
   *
   * @param conditions the list of conditions
   */
  public LogicalAnd(List<Condition> conditions) {
    mConditions = conditions;
  }

  @Override
  public IntervalSet evaluate(Interval interval, String path, InodeState state) {
    // collect all list of intervals for each condition
    List<IntervalSet> intervals = mConditions.stream().map(c -> c.evaluate(interval, path, state))
        .collect(Collectors.toList());
    return IntervalUtils.intersect(intervals);
  }

  @Override
  public String serialize() {
    return mConditions.stream().map(Condition::serialize).collect(Collectors.joining(" AND "));
  }
}
