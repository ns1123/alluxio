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
  private long mNextEvent;
  private long mTimeWindow;
  private long mNumEvents;
  private long mMaxEvents;

  /**
   * Creates a new instance of {@link ExponentialTimer}.
   *
   * @param maxEvents the maximum number of events
   * @param timeWindow the initial time window
   */
  public ExponentialTimer(long maxEvents, long timeWindow) {
    mTimeWindow = timeWindow;
    mNumEvents = 0;
    mMaxEvents = maxEvents;
    mNextEvent = System.currentTimeMillis();
  }

  /**
   * @return whether an event is ready
   */
  public boolean isReady() {
    return System.currentTimeMillis() >= mNextEvent;
  }

  /**
   * @return whether there are any events left
   */
  public boolean hasNext() {
    return mNumEvents < mMaxEvents;
  }

  /**
   * Generates the next event.
   *
   * @return the updated object
   */
  public ExponentialTimer next() {
    mNumEvents++;
    mNextEvent = System.currentTimeMillis() + mTimeWindow;
    long next = mTimeWindow << 1;
    // Account for overflow.
    if (next < mTimeWindow) {
      next = Integer.MAX_VALUE;
    }
    mTimeWindow = next;
    return this;
  }
}
