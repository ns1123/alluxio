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

<<<<<<< HEAD
import alluxio.Constants;
||||||| parent of c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
=======
import alluxio.security.authorization.AclEntry;
>>>>>>> c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.underfs.UfsStatus;
import alluxio.util.SecurityUtils;
import alluxio.wire.CommonOptions;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.List;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
public final class CreateDirectoryOptions extends CreatePathOptions<CreateDirectoryOptions> {
  private boolean mAllowExists;
  private long mTtl;
  private TtlAction mTtlAction;
  private UfsStatus mUfsStatus;
<<<<<<< HEAD
||||||| parent of c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)

=======
  private List<AclEntry> mDefaultAcl;

>>>>>>> c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
  }

  /**
   * Constructs an instance of {@link CreateDirectoryOptions} from {@link CreateDirectoryTOptions}.
   * The option of permission is constructed with the username obtained from thrift
   * transport.
   *
   * @param options the {@link CreateDirectoryTOptions} to use
   */
  public CreateDirectoryOptions(CreateDirectoryTOptions options) {
    this();
    if (options != null) {
      if (options.isSetCommonOptions()) {
        mCommonOptions = new CommonOptions(options.getCommonOptions());
      }
      mAllowExists = options.isAllowExists();
      mPersisted = options.isPersisted();
      mRecursive = options.isRecursive();
      mTtl = options.getTtl();
      mTtlAction = TtlAction.fromThrift(options.getTtlAction());
      if (SecurityUtils.isAuthenticationEnabled()) {
        mOwner = SecurityUtils.getOwnerFromThriftClient();
        mGroup = SecurityUtils.getGroupFromThriftClient();
      }
      if (options.isSetMode()) {
        mMode = new Mode(options.getMode());
      } else {
        mMode.applyDirectoryUMask();
      }
    }
  }

  private CreateDirectoryOptions() {
    super();
    mAllowExists = false;
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
    mMode.applyDirectoryUMask();
    mUfsStatus = null;
    mDefaultAcl = Collections.emptyList();
  }

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
<<<<<<< HEAD
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created directory
   *         should be kept around before it is automatically deleted or free
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
||||||| parent of c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
=======
   * @return the default ACL in the form of a list of default ACL Entries
   */
  public List<AclEntry> getDefaultAcl() {
    return mDefaultAcl;
  }

  /**
   * Sets the default ACL in the option.
   * @param defaultAcl a list of default ACL Entries
   * @return the updated options object
   */
  public CreateDirectoryOptions setDefaultAcl(List<AclEntry> defaultAcl) {
    mDefaultAcl = ImmutableList.copyOf(defaultAcl);
    return getThis();
  }

  /**
>>>>>>> c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
   * @return the {@link UfsStatus}
   */
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
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

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created directory should be kept around before it is automatically deleted
   * @return the updated options object
   */
  public CreateDirectoryOptions setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  /**
   * @param ttlAction the {@link TtlAction}; It informs the action to take when Ttl is expired;
   * @return the updated options object
   */
  public CreateDirectoryOptions setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return getThis();
  }

  /**
   * @param ufsStatus the {@link UfsStatus}; It sets the optional ufsStatus as an optimization
   * @return the updated options object
   */
  public CreateDirectoryOptions setUfsStatus(UfsStatus ufsStatus) {
    mUfsStatus = ufsStatus;
    return getThis();
  }

  @Override
  protected CreateDirectoryOptions getThis() {
    return this;
  }

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
<<<<<<< HEAD
    return Objects.equal(mAllowExists, that.mAllowExists) && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction) && Objects.equal(mUfsStatus, that.mUfsStatus);
||||||| parent of c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
    return Objects.equal(mAllowExists, that.mAllowExists)
        && Objects.equal(mUfsStatus, that.mUfsStatus);
=======
    return Objects.equal(mAllowExists, that.mAllowExists)
        && Objects.equal(mUfsStatus, that.mUfsStatus)
        && Objects.equal(mDefaultAcl, that.mDefaultAcl);
>>>>>>> c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
  }

  @Override
  public int hashCode() {
<<<<<<< HEAD
    return super.hashCode() + Objects.hashCode(mAllowExists, mTtl, mTtlAction, mUfsStatus);
||||||| parent of c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
    return super.hashCode() + Objects.hashCode(mAllowExists, mUfsStatus);
=======
    return super.hashCode() + Objects.hashCode(mAllowExists, mUfsStatus, mDefaultAcl);
>>>>>>> c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
  }

  @Override
  public String toString() {
<<<<<<< HEAD
    return toStringHelper()
        .add("allowExists", mAllowExists).add("ttl", mTtl)
        .add("ttlAction", mTtlAction)
        .add("ufsStatus", mUfsStatus)
        .toString();
||||||| parent of c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
    return toStringHelper().add("allowExists", mAllowExists)
        .add("ufsStatus", mUfsStatus).toString();
=======
    return toStringHelper().add("allowExists", mAllowExists)
        .add("ufsStatus", mUfsStatus)
        .add("defaultAcl", mDefaultAcl).toString();
>>>>>>> c8ec22a449... [ALLUXIO-3305] Fix loadMetadata bug related to ACL   (#7813)
  }
}
