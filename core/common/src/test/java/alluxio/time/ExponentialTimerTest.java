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

package alluxio.time;

import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link ExponentialTimer}.
 */
public class ExponentialTimerTest {

  /**
   * Tests the maximum number of attempts is respected.
   */
  @Test
  public void attempts() {
    int n = 10;
    ExponentialTimer timer = new ExponentialTimer(n, 0, 0);
    for (int i = 0; i < n; i++) {
      Assert.assertTrue(timer.isReady());
      Assert.assertTrue(timer.hasNext());
      timer.next();
    }
    Assert.assertFalse(timer.hasNext());
  }

  /**
   * Tests the exponential back-off logic.
   */
  @Test(timeout=2000)
  public void backoff() {
    int n = 10;
    ExponentialTimer timer = new ExponentialTimer(n, 1, 1000);
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      while (!timer.isReady()) {
        CommonUtils.sleepMs(10);
      }
      timer.next();
      long now = System.currentTimeMillis();
      Assert.assertTrue(now - start > (1 << i - 1));
    }
  }
}
