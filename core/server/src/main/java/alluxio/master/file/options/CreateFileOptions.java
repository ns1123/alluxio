/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.options;

import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.security.authorization.PermissionStatus;
import alluxio.thrift.CreateFileTOptions;

<<<<<<< HEAD
import java.io.IOException;

||||||| merged common ancestors
=======
import com.google.common.base.Objects;

import java.io.IOException;

>>>>>>> OPENSOURCE/master
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions extends CreatePathOptions<CreateFileOptions> {
  private long mBlockSizeBytes;
  private long mTtl;

  /**
   * @return the default {@link CreateFileOptions}
   * @throws IOException if I/O error occurs
   */
<<<<<<< HEAD
  public static CreateFileOptions defaults() throws IOException {
    return new CreateFileOptions();
||||||| merged common ancestors
  public static CreateFileOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private CreateFileOptions(CreateFileOptions.Builder builder) {
    mBlockSizeBytes = builder.mBlockSizeBytes;
    mOperationTimeMs = builder.mOperationTimeMs;
    mPersisted = builder.mPersisted;
    mRecursive = builder.mRecursive;
    mTtl = builder.mTtl;
=======
  public static CreateFileOptions defaults() {
    return new CreateFileOptions();
>>>>>>> OPENSOURCE/master
  }

  /**
   * Constructs an instance of {@link CreateFileOptions} from {@link CreateFileTOptions}. The
   * option of permission status is constructed with the username obtained from thrift transport.
   *
<<<<<<< HEAD
   * @param options the {@link CreateFileTOptions} to use
   * @throws IOException if an I/O error occurs
||||||| merged common ancestors
   * @param options Thrift options
=======
   * @param options the {@link CreateFileTOptions} to use
   * @throws IOException if it failed to retrieve users or groups from thrift transport
>>>>>>> OPENSOURCE/master
   */
  public CreateFileOptions(CreateFileTOptions options) throws IOException {
    super();
    mBlockSizeBytes = options.getBlockSizeBytes();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    mTtl = options.getTtl();
    mPermissionStatus =
        PermissionStatus.defaults().setUserFromThriftClient(MasterContext.getConf());
  }

  private CreateFileOptions() {
    super();
    mBlockSizeBytes = MasterContext.getConf().getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mTtl = Constants.NO_TTL;
  }

  private CreateFileOptions() throws IOException {
    super();
    mBlockSizeBytes = MasterContext.getConf().getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mTtl = Constants.NO_TTL;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
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
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted
   * @return the updated options object
   */
<<<<<<< HEAD
  public CreateFileOptions setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  @Override
  protected CreateFileOptions getThis() {
    return this;
||||||| merged common ancestors
  public boolean isRecursive() {
    return mRecursive;
=======
  public CreateFileOptions setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
>>>>>>> OPENSOURCE/master
  }

<<<<<<< HEAD
  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    return toStringHelper().add("blockSizeBytes", mBlockSizeBytes).add("ttl", mTtl).toString();
||||||| merged common ancestors
  /**
   * @return the TTL (time to live) value; it identifies duration (in milliseconds) the created file
   * should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
=======
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
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes)
        && Objects.equal(mTtl, that.mTtl);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mBlockSizeBytes, mTtl);
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("ttl", mTtl)
        .toString();
>>>>>>> OPENSOURCE/master
  }
}
