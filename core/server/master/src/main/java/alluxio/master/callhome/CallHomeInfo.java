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

package alluxio.master.callhome;

import com.google.common.base.Objects;

import java.util.Arrays;

/**
 * Call home information, can be encoded into JSON.
 */
public final class CallHomeInfo {
  private int mVersion;
  private String mLicenseKey;
  private String mClusterVersion;
  private boolean mFaultTolerant;
  private int mWorkerCount;
  private long mStartTime; // unix time, milliseconds
  private long mUptime; // milliseconds
  private String mUfsType;
  private long mUfsSize; // bytes
  private StorageTier[] mStorageTiers;

  /**
   * Creates a new instance of {@link CallHomeInfo}.
   */
  public CallHomeInfo() {}

  /**
   * @return the json format version
   */
  public int getVersion() {
    return mVersion;
  }

  /**
   * @return the license key
   */
  public String getLicenseKey() {
    return mLicenseKey;
  }

  /**
   * @return the cluster's start time in milliseconds
   */
  public long getStartTime() {
    return mStartTime;
  }

  /**
   * @return the cluster's uptime in milliseconds
   */
  public long getUptime() {
    return mUptime;
  }

  /**
   * @return the cluster's version
   */
  public String getClusterVersion() {
    return mClusterVersion;
  }

  /**
   * @return whether the cluster is in fault tolerant mode
   */
  public boolean getFaultTolerant() {
    return mFaultTolerant;
  }

  /**
   * @return the number of the workers
   */
  public int getWorkerCount() {
    return mWorkerCount;
  }

  /**
   * @return the under storage's type
   */
  public String getUfsType() {
    return mUfsType;
  }

  /**
   * @return the under storage's size in bytes
   */
  public long getUfsSize() {
    return mUfsSize;
  }

  /**
   * @return the storage tiers
   */
  public StorageTier[] getStorageTiers() {
    return mStorageTiers;
  }

  /**
   * @param version the json format version to use
   */
  public void setVersion(int version) {
    mVersion = version;
  }

  /**
   * @param key the license key to use
   */
  public void setLicenseKey(String key) {
    mLicenseKey = key;
  }

  /**
   * @param startTime the start time (in milliseconds) to use
   */
  public void setStartTime(long startTime) {
    mStartTime = startTime;
  }

  /**
   * @param uptime the uptime (in milliseconds) to use
   */
  public void setUptime(long uptime) {
    mUptime = uptime;
  }

  /**
   * @param version the version to use
   */
  public void setClusterVersion(String version) {
    mClusterVersion = version;
  }

  /**
   * @param faultTolerant whether the cluster is in fault tolerant mode
   */
  public void setFaultTolerant(boolean faultTolerant) {
    mFaultTolerant = faultTolerant;
  }

  /**
   * @param n the number of workers to use
   */
  public void setWorkerCount(int n) {
    mWorkerCount = n;
  }

  /**
   * @param type the under storage's type to use
   */
  public void setUfsType(String type) {
    mUfsType = type;
  }

  /**
   * @param size the under storage's size (in bytes) to use
   */
  public void setUfsSize(long size) {
    mUfsSize = size;
  }

  /**
   * @param storageTiers the storage tiers to use
   */
  public void setStorageTiers(StorageTier[] storageTiers) {
    mStorageTiers = new StorageTier[storageTiers.length];
    System.arraycopy(storageTiers, 0, mStorageTiers, 0, storageTiers.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CallHomeInfo)) {
      return false;
    }
    CallHomeInfo that = (CallHomeInfo) o;
    return Objects.equal(mVersion, that.mVersion)
        && Objects.equal(mLicenseKey, that.mLicenseKey)
        && Objects.equal(mStartTime, that.mStartTime)
        && Objects.equal(mUptime, that.mUptime)
        && Objects.equal(mClusterVersion, that.mClusterVersion)
        && Objects.equal(mFaultTolerant, that.mFaultTolerant)
        && Objects.equal(mWorkerCount, that.mWorkerCount)
        && Objects.equal(mUfsType, that.mUfsType)
        && Objects.equal(mUfsSize, that.mUfsSize)
        && Arrays.equals(mStorageTiers, that.mStorageTiers);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mVersion, mLicenseKey, mStartTime, mUptime, mClusterVersion,
        mFaultTolerant, mWorkerCount, mUfsType, mUfsSize, mStorageTiers);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("version", mVersion)
        .add("license", mLicenseKey)
        .add("start time", mStartTime)
        .add("uptime", mUptime)
        .add("version", mClusterVersion)
        .add("fault tolerant", mFaultTolerant)
        .add("worker count", mWorkerCount)
        .add("ufs type", mUfsType)
        .add("ufs size", mUfsSize)
        .add("storage tiers", mStorageTiers)
        .toString();
  }

  /**
   * Represents a tier in the tiered storage.
   */
  public static final class StorageTier {
    private String mAlias;
    private long mSize; // bytes

    /**
     * Creates a new instance of {@link StorageTier}.
     */
    public StorageTier() {}

    /**
     * @return the tier's alias
     */
    public String getAlias() {
      return mAlias;
    }

    /**
     * @return the tier's size in bytes
     */
    public long getSize() {
      return mSize;
    }

    /**
     * @param alias the tier's alias to use
     */
    public void setAlias(String alias) {
      mAlias = alias;
    }

    /**
     * @param size the tier's size (in bytes) to use
     */
    public void setSize(long size) {
      mSize = size;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StorageTier)) {
        return false;
      }
      StorageTier that = (StorageTier) o;
      return Objects.equal(mAlias, that.mAlias)
          && Objects.equal(mSize, that.mSize);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mAlias, mSize);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("alias", mAlias).add("size", mSize).toString();
    }
  }
}
