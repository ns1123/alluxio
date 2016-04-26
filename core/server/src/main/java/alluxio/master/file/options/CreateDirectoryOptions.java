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

<<<<<<< HEAD
||||||| merged common ancestors
import alluxio.Configuration;
import alluxio.master.MasterContext;
=======
import alluxio.master.MasterContext;
import alluxio.security.authorization.PermissionStatus;
>>>>>>> OPENSOURCE/master
import alluxio.thrift.CreateDirectoryTOptions;

import com.google.common.base.Objects;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
public final class CreateDirectoryOptions extends CreatePathOptions<CreateDirectoryOptions> {
  private boolean mAllowExists;

  /**
   * @return the default {@link CreateDirectoryOptions}
   * @throws IOException if I/O error occurs
   */
<<<<<<< HEAD
  public static CreateDirectoryOptions defaults() throws IOException {
    return new CreateDirectoryOptions();
||||||| merged common ancestors
  public static CreateDirectoryOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private boolean mAllowExists;
  private long mOperationTimeMs;
  private boolean mPersisted;
  private boolean mRecursive;

  private CreateDirectoryOptions(CreateDirectoryOptions.Builder builder) {
    mAllowExists = builder.mAllowExists;
    mOperationTimeMs = builder.mOperationTimeMs;
    mPersisted = builder.mPersisted;
    mRecursive = builder.mRecursive;
=======
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
>>>>>>> OPENSOURCE/master
  }

  /**
   * Constructs an instance of {@link CreateDirectoryOptions} from {@link CreateDirectoryTOptions}.
   * The option of permission status is constructed with the username obtained from thrift
   * transport.
   *
<<<<<<< HEAD
   * @param options the {@link CreateDirectoryTOptions} to use
   * @throws IOException if an I/O error occurs
||||||| merged common ancestors
   * @param options Thrift options
=======
   * @param options the {@link CreateDirectoryTOptions} to use
   * @throws IOException if it failed to retrieve users or groups from thrift transport
>>>>>>> OPENSOURCE/master
   */
<<<<<<< HEAD
  public CreateDirectoryOptions(CreateDirectoryTOptions options) throws IOException {
    super();
||||||| merged common ancestors
  public CreateDirectoryOptions(CreateDirectoryTOptions options) {
=======
  public CreateDirectoryOptions(CreateDirectoryTOptions options)
      throws IOException {
    super();
>>>>>>> OPENSOURCE/master
    mAllowExists = options.isAllowExists();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    mPermissionStatus =
        PermissionStatus.defaults().setUserFromThriftClient(MasterContext.getConf());
  }

  private CreateDirectoryOptions() {
    super();
    mAllowExists = false;
  }

  private CreateDirectoryOptions() throws IOException {
    super();
    mAllowExists = false;
  }

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @param allowExists the allowExists flag value to use; it specifies whether an exception
   *        should be thrown if the object being made already exists.
   * @return the updated options object
   */
  public CreateDirectoryOptions setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return this;
  }

  @Override
  protected CreateDirectoryOptions getThis() {
    return this;
  }

<<<<<<< HEAD
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateDirectoryOptions)) {
      return false;
    }
    CreateDirectoryOptions that = (CreateDirectoryOptions) o;
    // Do not require equal operation times for equality.
    return Objects.equal(mAllowExists, that.mAllowExists) && Objects
        .equal(mPersisted, that.mPersisted) && Objects.equal(mRecursive, that.mRecursive) && Objects
        .equal(mMountPoint, that.mMountPoint) && Objects.equal(mMetadataLoad, that.mMetadataLoad);
  }

  @Override
  public int hashCode() {
    // Omit operation time.
    return Objects.hashCode(mAllowExists, mPersisted, mRecursive, mMountPoint, mMetadataLoad);
  }

  @Override
  public String toString() {
    return toStringHelper().add("allowExists", mAllowExists).toString();
||||||| merged common ancestors
  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
=======
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateDirectoryOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    CreateDirectoryOptions that = (CreateDirectoryOptions) o;
    return Objects.equal(mAllowExists, that.mAllowExists);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mAllowExists);
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("allowExists", mAllowExists)
        .toString();
>>>>>>> OPENSOURCE/master
  }
}
