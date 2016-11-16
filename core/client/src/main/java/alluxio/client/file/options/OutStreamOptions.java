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

package alluxio.client.file.options;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.Permission;
import alluxio.util.CommonUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for writing a file.
 */
@PublicApi
@NotThreadSafe
public final class OutStreamOptions {
  private long mBlockSizeBytes;
  private long mTtl;
  private TtlAction mTtlAction;
  private FileWriteLocationPolicy mLocationPolicy;
  private WriteType mWriteType;
  private Permission mPermission;
<<<<<<< HEAD
  // ALLUXIO CS ADD
  private int mReplicationMax;
  private int mReplicationMin;
  // ALLUXIO CS END
=======
  private String mUfsPath;
>>>>>>> os/master

  /**
   * @return the default {@link OutStreamOptions}
   */
  public static OutStreamOptions defaults() {
    return new OutStreamOptions();
  }

  private OutStreamOptions() {
    mBlockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;

    try {
      mLocationPolicy = CommonUtils.createNewClassInstance(
          Configuration.<FileWriteLocationPolicy>getClass(
              PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[] {}, new Object[] {});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    mWriteType = Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    mPermission = Permission.defaults();
    try {
      // Set user and group from user login module, and apply default file UMask.
      mPermission.applyFileUMask().setOwnerFromLoginModule();
    } catch (IOException e) {
      // Fall through to system property approach
    }
    // ALLUXIO CS ADD
    mReplicationMax = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX);
    mReplicationMin = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);
    // ALLUXIO CS END
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the file write location policy
   */
  public FileWriteLocationPolicy getLocationPolicy() {
    return mLocationPolicy;
  }

  /**
   * @return the Alluxio storage type
   */
  public AlluxioStorageType getAlluxioStorageType() {
    return mWriteType.getAlluxioStorageType();
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in milliseconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return the {@link TtlAction}
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  /**
   * @return the under storage type
   */
  public UnderStorageType getUnderStorageType() {
    return mWriteType.getUnderStorageType();
  }

  /**
   * @return the permission
   */
  public Permission getPermission() {
    return mPermission;
  }

  // ALLUXIO CS ADD
  /**
   * @return the maximum number of block replication
   */
  public int getReplicationMax() {
    return mReplicationMax;
  }

  /**
   * @return the minimum number of block replication
   */
  public int getReplicationMin() {
    return mReplicationMin;
  }

  // ALLUXIO CS END
  /**
   * @return the ufs path
   */
  public String getUfsPath() {
    return mUfsPath;
  }

  /**
   * Sets the size of the block in bytes.
   *
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public OutStreamOptions setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * Sets the time to live.
   *
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, no matter
   *        whether the file is pinned
   * @return the updated options object
   */
  public OutStreamOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public OutStreamOptions setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return this;
  }

  /**
   * @param locationPolicy the file write location policy
   * @return the updated options object
   */
  public OutStreamOptions setLocationPolicy(FileWriteLocationPolicy locationPolicy) {
    mLocationPolicy = locationPolicy;
    return this;
  }

  /**
   * Sets the {@link WriteType}.
   *
   * @param writeType the {@link WriteType} to use for this operation. This will override both the
   *        {@link AlluxioStorageType} and {@link UnderStorageType}.
   * @return the updated options object
   */
  public OutStreamOptions setWriteType(WriteType writeType) {
    mWriteType = writeType;
    return this;
  }

  /**
   * @param ufsPath the ufs path
   * @return the updated options object
   */
  public OutStreamOptions setUfsPath(String ufsPath) {
    mUfsPath = ufsPath;
    return this;
  }

  /**
   * Sets the {@link Permission}.
   *
   * @param perm the permission
   * @return the updated options object
   */
  // TODO(binfan): remove or deprecate this method
  public OutStreamOptions setPermission(Permission perm) {
    mPermission = perm;
    return this;
  }

  // ALLUXIO CS ADD
  /**
   * @param replicationMax the maximum number of block replication
   * @return the updated options object
   */
  public OutStreamOptions setReplicationMax(int replicationMax) {
    mReplicationMax = replicationMax;
    return this;
  }

  /**
   * @param replicationMin the minimum number of block replication
   * @return the updated options object
   */
  public OutStreamOptions setReplicationMin(int replicationMin) {
    mReplicationMin = replicationMin;
    return this;
  }

  // ALLUXIO CS END
  /**
   * Sets the mode in {@link Permission}.
   *
   * @param mode the permission
   * @return the updated options object
   */
  public OutStreamOptions setMode(Mode mode) {
    if (mode != null) {
      mPermission.setMode(mode);
    }
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OutStreamOptions)) {
      return false;
    }
    OutStreamOptions that = (OutStreamOptions) o;
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction)
        && Objects.equal(mLocationPolicy, that.mLocationPolicy)
        && Objects.equal(mWriteType, that.mWriteType)
<<<<<<< HEAD
        // ALLUXIO CS ADD
        && Objects.equal(mReplicationMax, that.mReplicationMax)
        && Objects.equal(mReplicationMin, that.mReplicationMin)
        // ALLUXIO CS END
=======
        && Objects.equal(mUfsPath, that.mUfsPath)
>>>>>>> os/master
        && Objects.equal(mPermission, that.mPermission);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockSizeBytes,
        mTtl,
        mTtlAction,
        mLocationPolicy,
        mWriteType,
<<<<<<< HEAD
    // ALLUXIO CS REPLACE
    //     mPermission);
    // ALLUXIO CS WITH
        mReplicationMax,
        mReplicationMin,
=======
        mUfsPath,
>>>>>>> os/master
        mPermission);
    // ALLUXIO CS END
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("ttl", mTtl)
        .add("mTtlAction", mTtlAction)
        .add("locationPolicy", mLocationPolicy)
        .add("writeType", mWriteType)
        .add("permission", mPermission)
<<<<<<< HEAD
        // ALLUXIO CS ADD
        .add("replicationMax", mReplicationMax)
        .add("replicationMin", mReplicationMin)
        // ALLUXIO CS END
=======
        .add("ufsPath", mUfsPath)
>>>>>>> os/master
        .toString();
  }
}
