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

package alluxio.master.journal.raft;

import static org.junit.Assert.assertEquals;

import alluxio.clock.ManualClock;
import alluxio.time.ManualSleeper;
import alluxio.util.CommonUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link AnchoredFixedRateRunnable}.
 */
public class AnchoredFixedRateRunnableTest {

  private ManualClock mClock;
  private ManualSleeper mSleeper;
  private AtomicInteger mCount;

  private Thread mThread;

  @Before
  public void before() {
    mClock = new ManualClock();
    mSleeper = new ManualSleeper();
    mCount = new AtomicInteger(0);
  }

  @After
  public void after() throws InterruptedException {
    mThread.interrupt();
    mThread.join();
  }

  private void run(LocalTime anchorTime, int timesPerDay) {
    mThread = new Thread(new AnchoredFixedRateRunnable(anchorTime, timesPerDay,
        () -> mCount.incrementAndGet(), mClock, mSleeper));
    mThread.start();
  }

  private void setTime(int hour, int minute) {
    mClock.setTimeMs(LocalTime.of(hour, minute)
        .atDate(LocalDate.now())
        .toInstant(ZoneOffset.UTC)
        .toEpochMilli());
  }

  @Test
  public void frequencyOneUsesAnchorTime() throws InterruptedException {
    setTime(2, 30);
    run(LocalTime.of(4, 30), 1);

    checkedSleep(2, 0);
    assertEquals(0, mCount.get());
    mSleeper.wakeUp();

    for (int i = 1; i < 100; i++) {
      checkedSleep(24, 0);
      assertEquals(i, mCount.get());
      mSleeper.wakeUp();
    }
  }

  @Test
  public void frequencyTwo() throws InterruptedException {
    setTime(2, 30);
    run(LocalTime.of(4, 30), 2);

    checkedSleep(2, 0);
    assertEquals(0, mCount.get());
    mSleeper.wakeUp();

    for (int i = 1; i < 100; i++) {
      checkedSleep(12, 0);
      assertEquals(i, mCount.get());
      mSleeper.wakeUp();
    }
  }

  @Test
  public void highFrequency() throws InterruptedException {
    setTime(2, 30);
    run(LocalTime.of(4, 30), 24 * 6); // every 10 minutes.

    checkedSleep(0, 10);
    assertEquals(0, mCount.get());
    mSleeper.wakeUp();

    for (int i = 1; i < 100; i++) {
      checkedSleep(0, 10);
      assertEquals(i, mCount.get());
      mSleeper.wakeUp();
    }
  }

  @Test
  public void initialWrapToNextDay() throws InterruptedException {
    setTime(5, 30);
    run(LocalTime.of(4, 30), 1); // every 10 minutes.

    checkedSleep(23, 0);
    assertEquals(0, mCount.get());
    mSleeper.wakeUp();
    checkedSleep(24, 0);
    assertEquals(1, mCount.get());
  }

  @Test
  public void interruption() throws InterruptedException {
    setTime(5, 30);
    run(LocalTime.of(4, 30), 1); // every 10 minutes.
    mThread.interrupt();
    CommonUtils.waitFor("thread to stop sleeping", () -> !mSleeper.sleeping());
  }

  private void checkedSleep(int hours, int minutes) throws InterruptedException {
    long sleepTime = Duration.ofHours(hours).plus(Duration.ofMinutes(minutes)).toMillis();
    assertEquals(sleepTime, mSleeper.waitForSleep().toMillis());
    mClock.addTimeMs(sleepTime);
  }
}
