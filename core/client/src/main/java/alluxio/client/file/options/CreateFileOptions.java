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
<<<<<<< HEAD
import alluxio.exception.PreconditionMessage;
=======
>>>>>>> os/branch-1.3
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateFileTOptions;
import alluxio.util.CommonUtils;
import alluxio.wire.ThriftUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@PublicApi
@NotThreadSafe
public final class CreateFileOptions {
  private boolean mRecursive;
  private FileWriteLocationPolicy mLocationPolicy;
  private long mBlockSizeBytes;
  private long mTtl;
<<<<<<< HEAD
  private TtlAction mTtlAction;
=======
>>>>>>> os/branch-1.3
  private Mode mMode; // null if creating the file using system default mode
  private WriteType mWriteType;
  // ALLUXIO CS ADD
  private int mReplicationMax;
  private int mReplicationMin;
  // ALLUXIO CS END

  /**
   * @return the default {@link CreateFileOptions}
   */
  public static CreateFileOptions defaults() {
    return new CreateFileOptions();
  }

  private CreateFileOptions() {
    mRecursive = true;
    mBlockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    try {
      mLocationPolicy =
          CommonUtils.createNewClassInstance(Configuration.<FileWriteLocationPolicy>getClass(
              PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[] {}, new Object[] {});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    mWriteType = Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    // ALLUXIO CS ADD
    mReplicationMax = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX);
    mReplicationMin = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);
    // ALLUXIO CS END
    mTtl = Constants.NO_TTL;
<<<<<<< HEAD
    mTtlAction = TtlAction.DELETE;
=======
>>>>>>> os/branch-1.3
    mMode = null;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the location policy for writes to Alluxio storage
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
   * @return the TTL (time to live) value; it identifies duration (in milliseconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
<<<<<<< HEAD
   * @return the {@link TtlAction}
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  /**
=======
>>>>>>> os/branch-1.3
   * @return the mode of the file to create
   */
  public Mode getMode() {
    return mMode;
  }

  /**
   * @return the under storage type
   */
  public UnderStorageType getUnderStorageType() {
    return mWriteType.getUnderStorageType();
  }

  /**
   * @return whether or not the recursive flag is set
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public CreateFileOptions setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * @param locationPolicy the location policy to use
   * @return the updated options object
   */
  public CreateFileOptions setLocationPolicy(FileWriteLocationPolicy locationPolicy) {
    mLocationPolicy = locationPolicy;
    return this;
  }

  /**
   * @param mode the mode to be set
   * @return the updated options object
   */
  public CreateFileOptions setMode(Mode mode) {
    mMode = mode;
    return this;
  }

  /**
   * @param recursive whether or not to recursively create the file's parents
   * @return the updated options object
   */
  public CreateFileOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  // ALLUXIO CS ADD
  /**
   * @param replicationMax the maximum number of block replication
   * @return the updated options object
   */
  public CreateFileOptions setReplicationMax(int replicationMax) {
    Preconditions
        .checkArgument(replicationMax == Constants.REPLICATION_MAX_INFINITY || replicationMax >= 0,
            PreconditionMessage.INVALID_REPLICATION_MAX_VALUE);
    mReplicationMax = replicationMax;
    return this;
  }

  /**
   * @param replicationMin the minimum number of block replication
   * @return the updated options object
   */
  public CreateFileOptions setReplicationMin(int replicationMin) {
    Preconditions.checkArgument(replicationMin >= 0,
        PreconditionMessage.INVALID_REPLICATION_MIN_VALUE);
    mReplicationMin = replicationMin;
    return this;
  }

  // ALLUXIO CS END
  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, no matter whether
   *        the file is pinned
   * @return the updated options object
   */
  public CreateFileOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public CreateFileOptions setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return this;
  }

  /**
   * @param writeType the {@link WriteType} to use for this operation. This will override both the
   *        {@link AlluxioStorageType} and {@link UnderStorageType}.
   * @return the updated options object
   */
  public CreateFileOptions setWriteType(WriteType writeType) {
    mWriteType = writeType;
    return this;
  }

  /**
   * @return representation of this object in the form of {@link OutStreamOptions}
   */
  public OutStreamOptions toOutStreamOptions() {
<<<<<<< HEAD
    return OutStreamOptions.defaults()
        .setBlockSizeBytes(mBlockSizeBytes)
        .setLocationPolicy(mLocationPolicy)
        .setMode(mMode)
        // ALLUXIO CS ADD
        .setReplicationMax(mReplicationMax)
        .setReplicationMin(mReplicationMin)
        // ALLUXIO CS END
        .setTtl(mTtl)
        .setTtlAction(mTtlAction)
        .setWriteType(mWriteType);
=======
    return OutStreamOptions.defaults().setBlockSizeBytes(mBlockSizeBytes)
        .setLocationPolicy(mLocationPolicy).setMode(mMode).setTtl(mTtl).setWriteType(mWriteType);
>>>>>>> os/branch-1.3
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateFileOptions)) {
      return false;
    }
    CreateFileOptions that = (CreateFileOptions) o;
    return Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes)
        && Objects.equal(mLocationPolicy, that.mLocationPolicy)
<<<<<<< HEAD
        // ALLUXIO CS ADD
        && Objects.equal(mReplicationMax, that.mReplicationMax)
        && Objects.equal(mReplicationMin, that.mReplicationMin)
        // ALLUXIO CS END
=======
>>>>>>> os/branch-1.3
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction)
        && Objects.equal(mWriteType, that.mWriteType);
  }

  @Override
  public int hashCode() {
<<<<<<< HEAD
    // ALLUXIO CS REPLACE
    // return Objects.hashCode(mRecursive, mBlockSizeBytes, mLocationPolicy, mMode, mTtl,
    //     mTtlAction, mWriteType);
    // ALLUXIO CS WITH
    return Objects.hashCode(mRecursive, mBlockSizeBytes, mLocationPolicy, mMode, mReplicationMax,
        mReplicationMin, mTtl, mTtlAction, mWriteType);
    // ALLUXIO CS END
=======
    return Objects.hashCode(mRecursive, mBlockSizeBytes, mLocationPolicy, mMode, mTtl, mWriteType);
>>>>>>> os/branch-1.3
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("recursive", mRecursive)
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("locationPolicy", mLocationPolicy)
<<<<<<< HEAD
        // ALLUXIO CS ADD
        .add("replicationMax", mReplicationMax)
        .add("replicationMin", mReplicationMin)
        // ALLUXIO CS END
=======
>>>>>>> os/branch-1.3
        .add("mode", mMode)
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction)
        .add("writeType", mWriteType)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CreateFileTOptions toThrift() {
    CreateFileTOptions options = new CreateFileTOptions();
    options.setBlockSizeBytes(mBlockSizeBytes);
    options.setPersisted(mWriteType.getUnderStorageType().isSyncPersist());
    options.setRecursive(mRecursive);
    // ALLUXIO CS ADD
    options.setReplicationMax(mReplicationMax);
    options.setReplicationMin(mReplicationMin);
    // ALLUXIO CS END
    options.setTtl(mTtl);
<<<<<<< HEAD
    options.setTtlAction(ThriftUtils.toThrift(mTtlAction));
=======
>>>>>>> os/branch-1.3
    if (mMode != null) {
      options.setMode(mMode.toShort());
    }
    return options;
  }
}
