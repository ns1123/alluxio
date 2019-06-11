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
import alluxio.util.FormatUtils;

/**
 * A condition testing for the age of the path.
 */
public class OlderThan implements Condition {
  private final String mOlderThan;
  private final long mOlderThanMs;

  /**
   * Factory for creating instances.
   */
  public static class Factory implements ConditionFactory {
    @Override
    public Condition deserialize(String serialized) {
      try {
        if (serialized.startsWith("olderThan(") && serialized.endsWith(")")) {
          serialized = serialized.substring("olderThan(".length(), serialized.length() - 1);
          return new OlderThan(serialized);
        }
      } catch (Exception e) {
        // fall through
      }
      return null;
    }
  }

  /**
   * Creates an instance.
   *
   * @param olderThan the string representation of the older than time
   */
  public OlderThan(String olderThan) {
    mOlderThan = olderThan;
    mOlderThanMs = FormatUtils.parseTimeSize(olderThan);
  }

  @Override
  public IntervalSet evaluate(Interval interval, String path, InodeState state) {
    long olderThan = state.getCreationTimeMs() + mOlderThanMs;
    if (olderThan > interval.getEndMs()) {
      return IntervalSet.NEVER;
    }
    if (olderThan < interval.getStartMs()) {
      return new IntervalSet(interval);
    }
    return new IntervalSet(Interval.between(olderThan, interval.getEndMs()));
  }

  @Override
  public String serialize() {
    return "olderThan(" + mOlderThan + ")";
  }
}
