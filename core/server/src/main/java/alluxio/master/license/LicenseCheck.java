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

public class LicenseCheck implements JournalEntryRepresentable {
  private long mTimeMs;

  /**
   * Creates a new instance of {@link LicenseCheck}.
   */
  public LicenseCheck() {}

  /**
   * @return the time of the last successful check (in milliseconds)
   */
  public long getTime() {
    return mTimeMs;
  }

  /**
   * @param timeMs the time to use
   */
  public void setTime(long timeMs) {
    mTimeMs = timeMs;
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    LicenseCheckEntry licenseCheckEntry = LicenseCheckEntry.newBuilder().setTimeMs(mTimeMs).build();
    return Journal.JournalEntry.newBuilder().setLicenseCheck(licenseCheckEntry).build();
  }
}
