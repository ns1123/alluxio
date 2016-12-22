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

package alluxio.wire;

import com.google.common.base.Objects;

/**
 * License related information.
 */
public class LicenseInfo {
  private int mVersion;
  private String mName;
  private String mEmail;
  private String mKey;
  private String mChecksum;

  private long mLastCheckMs;
  private long mLastCheckSuccessMs;

  /**
   * Creates a new instance of {@link LicenseInfo}.
   */
  public LicenseInfo() {}

  /**
   * @return the license format version
   */
  public int getVersion() {
    return mVersion;
  }

  /**
   * @return the license name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the license email
   */
  public String getEmail() {
    return mEmail;
  }

  /**
   * @return the license key
   */
  public String getKey() {
    return mKey;
  }

  /**
   * @return the license checksum
   */
  public String getChecksum() {
    return mChecksum;
  }

  /**
   * @return the last check time in milliseconds
   */
  public long getLastCheckMs() {
    return mLastCheckMs;
  }

  /**
   * @return the last successful check time in milliseconds
   */
  public long getLastCheckSuccessMs() {
    return mLastCheckSuccessMs;
  }

  /**
   * @param version the version to use
   * @return the license info
   */
  public LicenseInfo setVersion(int version) {
    mVersion = version;
    return this;
  }

  /**
   * @param name the name to use
   * @return the license info
   */
  public LicenseInfo setName(String name) {
    mName = name;
    return this;
  }

  /**
   * @param email the email to use
   * @return the license info
   */
  public LicenseInfo setEmail(String email) {
    mEmail = email;
    return this;
  }

  /**
   * @param key the key to use
   * @return the license info
   */
  public LicenseInfo setKey(String key) {
    mKey = key;
    return this;
  }

  /**
   * @param checksum the checksum to use
   * @return the license info
   */
  public LicenseInfo setChecksum(String checksum) {
    mChecksum = checksum;
    return this;
  }

  /**
   * @param lastCheckMs the last check time in milliseconds
   * @return the license info
   */
  public LicenseInfo setLastCheckMs(long lastCheckMs) {
    mLastCheckMs = lastCheckMs;
    return this;
  }

  /**
   * @param lastCheckSuccessMs the last successful check time in milliseconds
   * @return the license info
   */
  public LicenseInfo setLastCheckSuccessMs(long lastCheckSuccessMs) {
    mLastCheckSuccessMs = lastCheckSuccessMs;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LicenseInfo)) {
      return false;
    }
    LicenseInfo that = (LicenseInfo) o;
    return Objects.equal(mVersion, that.mVersion)
        && Objects.equal(mName, that.mName)
        && Objects.equal(mEmail, that.mEmail)
        && Objects.equal(mKey, that.mKey)
        && Objects.equal(mChecksum, that.mChecksum)
        && mLastCheckMs == that.mLastCheckMs
        && mLastCheckSuccessMs == that.mLastCheckSuccessMs;
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(mVersion, mName, mEmail, mKey, mChecksum, mLastCheckMs, mLastCheckSuccessMs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("version", mVersion).add("name", mName)
        .add("email", mEmail).add("key", mKey).add("checksum", mChecksum)
        .add("last check", mLastCheckMs)
        .add("last successful check", mLastCheckSuccessMs).toString();
  }
}
