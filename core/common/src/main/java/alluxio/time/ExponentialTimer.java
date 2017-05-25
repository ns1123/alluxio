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

import alluxio.retry.RetryPolicy;
import alluxio.retry.ExponentialBackoffRetry;

/**
 * The {@link ExponentialTimer} can be used for generating a sequence of events that are
 * exponentially distributed in time.
 *
 * The intended usage is to associate the timer with an operation that should to be re-attempted
 * if it fails using exponential back-off. For instance, an event loop iterates over scheduled
 * operations each of which is associated with a timer and the event loop can use the timer to check
 * if the operation is ready to be attempted and whether it should be re-attempted again in the
 * future in case it fails.
 *
 * <pre>
 * while (true) {
 *   Operation op = operations.pop();
 *   if (op.getTimer().isReady()) {
 *     boolean success = op.run();
 *     if (!success && op.getTimer().hasNext()) {
 *       op.getTimer().next();
 *       operations.push(op);
 *     }
 *   }
 *   Thread.sleep(10);
 * }
 * </pre>
 *
 * Note that in the above scenario, the {@link ExponentialBackoffRetry} policy is not applicable
 * because the {@link RetryPolicy#attemptRetry()} is blocking.
 */
public class ExponentialTimer {
  /** The time of the next event. */
  private long mNextEventMs;
  /** The current wait time (in milliseconds) between events. */
  private long mWaitTimeMs;
  /** The maximum wait time (in milliseconds) between events. */
  private final long mMaxWaitTimeMs;
  /** The number of generated events. */
  private long mNumEvents;
  /** The maximum number of events. */
  private final long mMaxNumEvents;

  /**
   * Creates a new instance of {@link ExponentialTimer}.
   *
   * @param maxEvents the maximum number of events
   * @param initialWaitTimesMs the initial wait time (in milliseconds)
   * @param maxWaitTimeMs the initial wait time (in milliseconds)
   */
  public ExponentialTimer(long maxEvents, long initialWaitTimesMs, long maxWaitTimeMs) {
    mMaxWaitTimeMs = maxWaitTimeMs;
    mWaitTimeMs = Math.min(initialWaitTimesMs, maxWaitTimeMs);
    mNumEvents = 0;
    mMaxNumEvents = maxEvents;
    mNextEventMs = System.currentTimeMillis();
  }

  /**
   * @return whether an event is ready
   */
  public boolean isReady() {
    return System.currentTimeMillis() >= mNextEventMs;
  }

  /**
   * @return whether there are any events left
   */
  public boolean hasNext() {
    return mNumEvents < mMaxNumEvents;
  }

  /**
   * Generates the next event.
   *
   * @return the updated object
   */
  public ExponentialTimer next() {
    mNumEvents++;
    mNextEventMs = System.currentTimeMillis() + mWaitTimeMs;
    long next = Math.min(mWaitTimeMs * 2, mMaxWaitTimeMs);
    // Account for overflow.
    if (next < mWaitTimeMs) {
      next = Integer.MAX_VALUE;
    }
    mWaitTimeMs = next;
    return this;
  }
}
