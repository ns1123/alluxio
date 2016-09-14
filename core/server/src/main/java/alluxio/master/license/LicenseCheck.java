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
  private long mLastMs;
  private long mLastSuccessMs;

  /**
   * Creates a new instance of {@link LicenseCheck}.
   */
  public LicenseCheck() {}

  /**
   * @return the time of the last check (as RFC3339)
   */
  public String getLast() {
    Date date = new Date(mLastMs);
    DateFormat formatter = new SimpleDateFormat(License.TIME_FORMAT);
    return formatter.format(date);
  }

  /**
   * @return the time of the last check (in milliseconds)
   */
  public long getLastMs() {
    return mLastMs;
  }

  /**
   * @return the time of the last successful check (as RFC3339)
   */
  public String getLastSuccess() {
    Date date = new Date(mLastSuccessMs);
    DateFormat formatter = new SimpleDateFormat(License.TIME_FORMAT);
    return formatter.format(date);
  }

  /**
   * @return the time of the last successful check (in milliseconds)
   */
  public long getLastSuccessMs() {
    return mLastSuccessMs;
  }

  /**
   * @param lastMs the time of the last check
   */
  public void setLast(long lastMs) {
    mLastMs = lastMs;
  }

  /**
   * @param lastSuccessMs the time of the last successful check to use
   */
  public void setLastSuccess(long lastSuccessMs) {
    mLastSuccessMs = lastSuccessMs;
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    LicenseCheckEntry licenseCheckEntry =
        LicenseCheckEntry.newBuilder().setTimeMs(mLastSuccessMs).build();
    return Journal.JournalEntry.newBuilder().setLicenseCheck(licenseCheckEntry).build();
  }
}
