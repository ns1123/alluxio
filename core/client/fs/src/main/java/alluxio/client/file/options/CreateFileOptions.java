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
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateFileTOptions;
import alluxio.util.CommonUtils;
import alluxio.wire.CommonOptions;
import alluxio.wire.TtlAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class CreateFileOptions {
  // ALLUXIO CS ADD
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(CreateFileOptions.class);
  // ALLUXIO CS END
  private CommonOptions mCommonOptions;
  private boolean mRecursive;
  private FileWriteLocationPolicy mLocationPolicy;
  private long mBlockSizeBytes;
  private Mode mMode;
  private int mWriteTier;
  private WriteType mWriteType;
  // ALLUXIO CS ADD
  private int mReplicationDurable;
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
    mCommonOptions = CommonOptions.defaults()
        .setTtl(Configuration.getLong(PropertyKey.USER_FILE_CREATE_TTL))
        .setTtlAction(Configuration.getEnum(PropertyKey.USER_FILE_CREATE_TTL_ACTION,
            TtlAction.class));
    mRecursive = true;
    mBlockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mLocationPolicy =
        CommonUtils.createNewClassInstance(Configuration.<FileWriteLocationPolicy>getClass(
            PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[] {}, new Object[] {});
    mWriteTier = Configuration.getInt(PropertyKey.USER_FILE_WRITE_TIER_DEFAULT);
    mWriteType = Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    // ALLUXIO CS ADD
    mReplicationDurable = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_DURABLE);
    mReplicationMax = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX);
    mReplicationMin = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);
    // ALLUXIO CS END
    mMode = Mode.defaults().applyFileUMask();
  }

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the location policy used when storing data to Alluxio
   */
  @JsonIgnore
  public FileWriteLocationPolicy getLocationPolicy() {
    return mLocationPolicy;
  }

  /**
   * @return the location policy class used when storing data to Alluxio
   */
  public String getLocationPolicyClass() {
    return mLocationPolicy.getClass().getCanonicalName();
  }

  // ALLUXIO CS ADD
  /**
   * @return the number of block replication for durable write
   */
  public int getReplicationDurable() {
    return mReplicationDurable;
  }

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
    return getCommonOptions().getTtl();
  }

  /**
   * @return the {@link TtlAction}
   */
  public TtlAction getTtlAction() {
    return getCommonOptions().getTtlAction();
  }

  /**
   * @return the mode of the file to create
   */
  public Mode getMode() {
    return mMode;
  }

  /**
   * @return the write tier
   */
  public int getWriteTier() {
    return mWriteTier;
  }

  /**
   * @return the write type
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @return whether or not the recursive flag is set
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public CreateFileOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
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
  @JsonIgnore
  public CreateFileOptions setLocationPolicy(FileWriteLocationPolicy locationPolicy) {
    mLocationPolicy = locationPolicy;
    return this;
  }

  /**
   * @param className the location policy class to use when storing data to Alluxio
   * @return the updated options object
   */
  public CreateFileOptions setLocationPolicyClass(String className) {
    try {
      @SuppressWarnings("unchecked") Class<FileWriteLocationPolicy> clazz =
          (Class<FileWriteLocationPolicy>) Class.forName(className);
      mLocationPolicy = CommonUtils.createNewClassInstance(clazz, new Class[] {}, new Object[] {});
      return this;
    } catch (Exception e) {
      Throwables.propagate(e);
    }
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
   * @param replicationDurable the number of block replication for durable write
   * @return the updated options object
   */
  public CreateFileOptions setReplicationDurable(int replicationDurable) {
    mReplicationDurable = replicationDurable;
    return this;
  }

  /**
   * @param replicationMax the maximum number of block replication
   * @return the updated options object
   */
  public CreateFileOptions setReplicationMax(int replicationMax) {
    com.google.common.base.Preconditions
        .checkArgument(replicationMax == alluxio.Constants.REPLICATION_MAX_INFINITY || replicationMax >= 0,
            alluxio.exception.PreconditionMessage.INVALID_REPLICATION_MAX_VALUE);
    mReplicationMax = replicationMax;
    return this;
  }

  /**
   * @param replicationMin the minimum number of block replication
   * @return the updated options object
   */
  public CreateFileOptions setReplicationMin(int replicationMin) {
    com.google.common.base.Preconditions.checkArgument(replicationMin >= 0,
        alluxio.exception.PreconditionMessage.INVALID_REPLICATION_MIN_VALUE);
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
    getCommonOptions().setTtl(ttl);
    return this;
  }

  /**
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public CreateFileOptions setTtlAction(TtlAction ttlAction) {
    getCommonOptions().setTtlAction(ttlAction);
    return this;
  }

  /**
   * @param writeTier the write tier to use for this operation
   * @return the updated options object
   */
  public CreateFileOptions setWriteTier(int writeTier) {
    // ALLUXIO CS REPLACE
    // mWriteTier = writeTier;
    // ALLUXIO CS WITH
    if (writeTier != mWriteTier) {
      LOG.warn("Specific write tiers are not supported in this version.");
    }
    // ALLUXIO CS END
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
    return OutStreamOptions.defaults()
        .setBlockSizeBytes(mBlockSizeBytes)
        .setLocationPolicy(mLocationPolicy)
        .setMode(mMode)
        // ALLUXIO CS ADD
        .setReplicationDurable(mReplicationDurable)
        .setReplicationMax(mReplicationMax)
        .setReplicationMin(mReplicationMin)
        // ALLUXIO CS END
        .setWriteTier(mWriteTier)
        .setWriteType(mWriteType);
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
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes)
        && Objects.equal(mLocationPolicy, that.mLocationPolicy)
        // ALLUXIO CS ADD
        && Objects.equal(mReplicationDurable, that.mReplicationDurable)
        && Objects.equal(mReplicationMax, that.mReplicationMax)
        && Objects.equal(mReplicationMin, that.mReplicationMin)
        // ALLUXIO CS END
        && Objects.equal(mMode, that.mMode)
        && mWriteTier == that.mWriteTier
        && Objects.equal(mWriteType, that.mWriteType);
  }

  @Override
  public int hashCode() {
    // ALLUXIO CS REPLACE
    // return Objects
    //     .hashCode(mRecursive, mBlockSizeBytes, mLocationPolicy, mMode, mWriteTier,
    //         mWriteType, mCommonOptions);
    // ALLUXIO CS WITH
    return Objects.hashCode(mRecursive, mBlockSizeBytes, mLocationPolicy, mMode,
        mReplicationDurable, mReplicationMax, mReplicationMin, mWriteTier,
        mWriteType, mCommonOptions);
    // ALLUXIO CS END
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("recursive", mRecursive)
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("locationPolicy", mLocationPolicy)
        // ALLUXIO CS ADD
        .add("replicationDurable", mReplicationDurable)
        .add("replicationMax", mReplicationMax)
        .add("replicationMin", mReplicationMin)
        // ALLUXIO CS END
        .add("mode", mMode)
        .add("writeTier", mWriteTier)
        .add("writeType", mWriteType)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CreateFileTOptions toThrift() {
    CreateFileTOptions options = new CreateFileTOptions();
    options.setBlockSizeBytes(mBlockSizeBytes);
    options.setPersisted(mWriteType.isThrough());
    options.setRecursive(mRecursive);
    // ALLUXIO CS ADD
    options.setReplicationDurable(mReplicationDurable);
    options.setReplicationMax(mReplicationMax);
    options.setReplicationMin(mReplicationMin);
    // ALLUXIO CS END
    if (mMode != null) {
      options.setMode(mMode.toShort());
    }
    options.setCommonOptions(mCommonOptions.toThrift());
    return options;
  }
}
