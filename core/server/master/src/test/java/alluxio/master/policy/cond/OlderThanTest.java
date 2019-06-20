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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.master.policy.meta.interval.IntervalSet;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link OlderThan}.
 */
public class OlderThanTest {
  @Test
  public void evaluate() {
    long creationTimeMs = System.currentTimeMillis();
    InodeState inode = Mockito.mock(InodeState.class);
    when(inode.getCreationTimeMs()).thenReturn(creationTimeMs);

    long olderThanSeconds = 1;
    OlderThan cond = new OlderThan(String.format("%ds", olderThanSeconds));

    Interval toEvaluate = Interval.before(creationTimeMs);
    IntervalSet intervals = cond.evaluate(toEvaluate, "/ignore", inode);
    assertEquals(1, intervals.getIntervals().size());
    assertEquals(Interval.NEVER, intervals.getIntervals().get(0));

    toEvaluate = Interval.after(creationTimeMs + olderThanSeconds * Constants.SECOND_MS + 1);
    intervals = cond.evaluate(toEvaluate, "/ignore", inode);
    assertEquals(1, intervals.getIntervals().size());
    assertEquals(toEvaluate, intervals.getIntervals().get(0));

    long startMs = creationTimeMs + olderThanSeconds * Constants.SECOND_MS - 1;
    long endMs = startMs + Constants.DAY_MS;
    toEvaluate = Interval.between(startMs, endMs);
    intervals = cond.evaluate(toEvaluate, "/ignore", inode);
    assertEquals(1, intervals.getIntervals().size());
    assertEquals(
        Interval.between(creationTimeMs + olderThanSeconds * Constants.SECOND_MS, endMs),
        intervals.getIntervals().get(0));
  }
}
