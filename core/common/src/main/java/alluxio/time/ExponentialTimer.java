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

/**
 * The {@link ExponentialTimer} can be used for generating a sequence of events that are
 * exponentially distributed in time.
 */
public class ExponentialTimer {
  /** The time of the next event. */
  private long mNextEventMs;
  /** The current wait time (in milliseconds). */
  private long mWaitTimeMs;
  /** The maximum wait time (in milliseconds). */
  private long mMaxWaitTimeMs;
  /** The current number of events. */
  private long mNumEvents;
  /** The maximum number of events. */
  private long mMaxNumEvents;

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
    long next = Math.min(mWaitTimeMs << 1, mMaxWaitTimeMs);
    // Account for overflow.
    if (next < mWaitTimeMs) {
      next = Integer.MAX_VALUE;
    }
    mWaitTimeMs = next;
    return this;
  }
}
