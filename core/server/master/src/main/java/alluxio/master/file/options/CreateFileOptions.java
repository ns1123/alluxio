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

package alluxio.master.file.options;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateFileTOptions;
import alluxio.util.SecurityUtils;
import alluxio.wire.CommonOptions;
import alluxio.wire.ThriftUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions extends CreatePathOptions<CreateFileOptions> {
  private long mBlockSizeBytes;
  // ALLUXIO CS ADD
  private int mReplicationDurable;
  private int mReplicationMax;
  private int mReplicationMin;
  private boolean mEncrypted;
  // ALLUXIO CS END
  private long mTtl;
  private TtlAction mTtlAction;
  private boolean mCacheable;

  /**
   * @return the default {@link CreateFileOptions}
   */
  public static CreateFileOptions defaults() {
    return new CreateFileOptions();
  }

  /**
   * Constructs an instance of {@link CreateFileOptions} from {@link CreateFileTOptions}. The option
   * of permission is constructed with the username obtained from thrift transport.
   *
   * @param options the {@link CreateFileTOptions} to use
   */
  public CreateFileOptions(CreateFileTOptions options) {
    this();
    if (options != null) {
      if (options.isSetCommonOptions()) {
        mCommonOptions = new CommonOptions(options.getCommonOptions());
      }
      mBlockSizeBytes = options.getBlockSizeBytes();
      mPersisted = options.isPersisted();
      mRecursive = options.isRecursive();
      // ALLUXIO CS ADD
      mReplicationDurable = options.getReplicationDurable();
      mReplicationMax = options.getReplicationMax();
      mReplicationMin = options.getReplicationMin();
      mEncrypted = Configuration.getBoolean(PropertyKey.SECURITY_ENCRYPTION_ENABLED);
      // ALLUXIO CS END
      mTtl = options.getTtl();
      mTtlAction = ThriftUtils.fromThrift(options.getTtlAction());
      if (SecurityUtils.isAuthenticationEnabled()) {
        mOwner = SecurityUtils.getOwnerFromThriftClient();
        mGroup = SecurityUtils.getGroupFromThriftClient();
      }
      if (options.isSetMode()) {
        mMode = new Mode(options.getMode());
      } else {
        mMode.applyFileUMask();
      }
    }
  }

  private CreateFileOptions() {
    super();
    mBlockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    // ALLUXIO CS ADD
    mReplicationDurable = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_DURABLE);
    mReplicationMax = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX);
    mReplicationMin = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);
    mEncrypted = Configuration.getBoolean(PropertyKey.SECURITY_ENCRYPTION_ENABLED);
    // ALLUXIO CS END
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
    mMode.applyFileUMask();
    mCacheable = false;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
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

  /**
   * @return true if the file is encrypted, false otherwise
   */
  public boolean isEncrypted() {
    return mEncrypted;
  }

  // ALLUXIO CS END
  /**
   * @return true if file is cacheable
   */
  public boolean isCacheable() {
    return mCacheable;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
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
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public CreateFileOptions setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
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
    mReplicationMax = replicationMax;
    return this;
  }

  /**
   * @param replicationMin the minimum number of block replication
   * @return the updated options object
   */
  public CreateFileOptions setReplicationMin(int replicationMin) {
    mReplicationMin = replicationMin;
    return this;
  }

  /**
   * @param encrypted the encrypted flag value to use
   * @return the updated options object
   */
  public CreateFileOptions setEncrypted(boolean encrypted) {
    mEncrypted = encrypted;
    return this;
  }

  // ALLUXIO CS END
  /**
   * @param cacheable true if the file is cacheable, false otherwise
   * @return the updated options object
   */
  public CreateFileOptions setCacheable(boolean cacheable) {
    mCacheable = cacheable;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted
   * @return the updated options object
   */
  public CreateFileOptions setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  /**
   * @param ttlAction the {@link TtlAction}; It informs the action to take when Ttl is expired;
   * @return the updated options object
   */
  public CreateFileOptions setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return getThis();
  }

  @Override
  protected CreateFileOptions getThis() {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateFileOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    CreateFileOptions that = (CreateFileOptions) o;
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes) && Objects.equal(mTtl, that.mTtl)
        // ALLUXIO CS ADD
        && Objects.equal(mReplicationDurable, that.mReplicationDurable)
        && Objects.equal(mReplicationMax, that.mReplicationMax)
        && Objects.equal(mReplicationMin, that.mReplicationMin)
        && Objects.equal(mEncrypted, that.mEncrypted)
        // ALLUXIO CS END
        && Objects.equal(mTtlAction, that.mTtlAction) && Objects.equal(mCacheable, that.mCacheable);
  }

  @Override
  public int hashCode() {
    // ALLUXIO CS REPLACE
    // return super.hashCode() + Objects.hashCode(mBlockSizeBytes, mTtl, mTtlAction, mCacheable);
    // ALLUXIO CS WITH
    return super.hashCode() + Objects.hashCode(mBlockSizeBytes, mReplicationDurable,
        mReplicationMax, mReplicationMin, mEncrypted, mTtl, mTtlAction, mCacheable);
    // ALLUXIO CS END
  }

  @Override
  public String toString() {
    return toStringHelper().add("blockSizeBytes", mBlockSizeBytes).add("ttl", mTtl)
        // ALLUXIO CS ADD
        .add("replicationDurable", mReplicationDurable)
        .add("replicationMax", mReplicationMax)
        .add("replicationMin", mReplicationMin)
        .add("encrypted", mEncrypted)
        // ALLUXIO CS END
        .add("ttlAction", mTtlAction).add("cacheable", mCacheable).toString();
  }
}
