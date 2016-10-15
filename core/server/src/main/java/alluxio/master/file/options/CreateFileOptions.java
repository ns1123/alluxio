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
import alluxio.security.authorization.Permission;
import alluxio.thrift.CreateFileTOptions;
import alluxio.wire.ThriftUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions extends CreatePathOptions<CreateFileOptions> {
  private long mBlockSizeBytes;
  // ALLUXIO CS ADD
  private int mReplicationMax;
  private int mReplicationMin;
  // ALLUXIO CS END
  private long mTtl;
  private TtlAction mTtlAction;

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
   * @throws IOException if it failed to retrieve users or groups from thrift transport
   */
  public CreateFileOptions(CreateFileTOptions options) throws IOException {
    super();
    mBlockSizeBytes = options.getBlockSizeBytes();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    // ALLUXIO CS ADD
    mReplicationMax = options.getReplicationMax();
    mReplicationMin = options.getReplicationMin();
    // ALLUXIO CS END
    mTtl = options.getTtl();
    mTtlAction = ThriftUtils.fromThrift(options.getTtlAction());
    mPermission = Permission.defaults().setOwnerFromThriftClient();
  }

  private CreateFileOptions() {
    super();
    mBlockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    // ALLUXIO CS ADD
    mReplicationMax = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MAX);
    mReplicationMin = Configuration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);
    // ALLUXIO CS END
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
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

  // ALLUXIO CS END
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
<<<<<<< HEAD
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes) && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction);
||||||| merged common ancestors
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes)
        && Objects.equal(mTtl, that.mTtl);
=======
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes) && Objects.equal(mTtl, that.mTtl)
        // ALLUXIO CS ADD
        && Objects.equal(mReplicationMax, that.mReplicationMax)
        && Objects.equal(mReplicationMin, that.mReplicationMin)
        // ALLUXIO CS END
        && Objects.equal(mTtlAction, that.mTtlAction);
>>>>>>> upstream/enterprise-1.3
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mBlockSizeBytes, mTtl, mTtlAction);
  }

  @Override
  public String toString() {
<<<<<<< HEAD
    return toStringHelper().add("blockSizeBytes", mBlockSizeBytes).add("ttl", mTtl)
        .add("ttlAction", mTtlAction).toString();
||||||| merged common ancestors
    return toStringHelper()
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("ttl", mTtl)
        .toString();
=======
    return toStringHelper().add("blockSizeBytes", mBlockSizeBytes).add("ttl", mTtl)
        // ALLUXIO CS ADD
        .add("replicationMax", mReplicationMax)
        .add("replicationMin", mReplicationMin)
        // ALLUXIO CS END
        .add("ttlAction", mTtlAction).toString();
>>>>>>> upstream/enterprise-1.3
  }
}
