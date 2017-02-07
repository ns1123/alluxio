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

package alluxio.master.license;

import alluxio.LicenseConstants;
import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.License.LicenseCheckEntry;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Represents a condensed license check history.
 */
public class LicenseCheck implements JournalEntryRepresentable {
  private long mLastCheckMs;
  private long mLastCheckSuccessMs;

  /**
   * Creates a new instance of {@link LicenseCheck}.
   */
  public LicenseCheck() {}

  /**
   * @return the time of the last check (as RFC3339)
   */
  public String getLastCheck() {
    Date date = new Date(mLastCheckMs);
    DateFormat formatter = new SimpleDateFormat(License.TIME_FORMAT);
    return formatter.format(date);
  }

  /**
   * @return the time of the last check (in milliseconds)
   */
  public long getLastCheckMs() {
    return mLastCheckMs;
  }

  /**
   * @return the time of the last successful check (as RFC3339)
   */
  public String getLastCheckSuccess() {
    Date date = new Date(mLastCheckSuccessMs);
    DateFormat formatter = new SimpleDateFormat(License.TIME_FORMAT);
    return formatter.format(date);
  }

  /**
   * @return the time of the last successful check (in milliseconds)
   */
  public long getLastCheckSuccessMs() {
    return mLastCheckSuccessMs;
  }

  /**
   * @param lastCheckMs the time of the last check
   */
  public void setLastCheck(long lastCheckMs) {
    mLastCheckMs = lastCheckMs;
  }

  /**
   * @param lastCheckSuccessMs the time of the last successful check to use
   */
  public void setLastCheckSuccess(long lastCheckSuccessMs) {
    mLastCheckSuccessMs = lastCheckSuccessMs;
  }

  /**
   * @return whether the last license check was successful
   */
  public boolean isLastCheckSuccess() {
    return mLastCheckSuccessMs != 0 && mLastCheckMs == mLastCheckSuccessMs;
  }

  /**
   * @return the time of the grace period end (in milliseconds)
   */
  public long getGracePeriodEndMs() {
    return mLastCheckSuccessMs + Long.parseLong(LicenseConstants.LICENSE_GRACE_PERIOD_MS);
  }

  /**
   * @return the time of the grace period end (in RFC3339)
   */
  public String getGracePeriodEnd() {
    Date date = new Date(getGracePeriodEndMs());
    DateFormat formatter = new SimpleDateFormat(License.TIME_FORMAT);
    return formatter.format(date);
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    LicenseCheckEntry licenseCheckEntry =
        LicenseCheckEntry.newBuilder().setTimeMs(mLastCheckSuccessMs).build();
    return Journal.JournalEntry.newBuilder().setLicenseCheck(licenseCheckEntry).build();
  }
}
