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

import alluxio.thrift.GetStatusTOptions;
import alluxio.wire.CommonOptions;
import alluxio.wire.LoadMetadataType;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for getStatus.
 */
@NotThreadSafe
public final class GetStatusOptions {
  private CommonOptions mCommonOptions;
  private LoadMetadataType mLoadMetadataType;
  // ALLUXIO CS ADD
  private alluxio.security.authorization.Mode.Bits mAccessMode;
  // ALLUXIO CS END

  /**
   * @return the default {@link GetStatusOptions}
   */
  public static GetStatusOptions defaults() {
    return new GetStatusOptions();
  }

  private GetStatusOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
    mLoadMetadataType = LoadMetadataType.Once;
    // ALLUXIO CS ADD
    mAccessMode = alluxio.security.authorization.Mode.Bits.READ;
    // ALLUXIO CS END
  }

  /**
   * Create an instance of {@link GetStatusOptions} from a {@link GetStatusTOptions}.
   *
   * @param options the thrift representation of getFileInfo options
   */
  public GetStatusOptions(GetStatusTOptions options) {
    this();
    if (options != null) {
      if (options.isSetCommonOptions()) {
        mCommonOptions = new CommonOptions(options.getCommonOptions());
      }
      if (options.isSetLoadMetadataType()) {
        mLoadMetadataType = LoadMetadataType.fromThrift(options.getLoadMetadataType());
      }
      // ALLUXIO CS ADD
      if (options.isSetAccessMode()) {
        mAccessMode = alluxio.security.authorization.Mode.Bits.fromShort(options.getAccessMode());
      }
      // ALLUXIO CS END
    }
  }

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return the load metadata type
   */
  public LoadMetadataType getLoadMetadataType() {
    return mLoadMetadataType;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public GetStatusOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
  }

  /**
   * Sets the {@link GetStatusOptions#mLoadMetadataType}.
   *
   * @param loadMetadataType the load metadata type
   * @return the updated options
   */
  public GetStatusOptions setLoadMetadataType(LoadMetadataType loadMetadataType) {
    mLoadMetadataType = loadMetadataType;
    return this;
  }

  // ALLUXIO CS ADD

  /**
   * @return the access mode
   */
  public alluxio.security.authorization.Mode.Bits getAccessMode() {
    return mAccessMode;
  }

  /**
   * @param accessMode the access mode
   * @return the updated options
   */
  public GetStatusOptions setAccessMode(alluxio.security.authorization.Mode.Bits accessMode) {
    // Currently only single READ or single WRITE access mode request is valid.
    // EXECUTE access mode or READ_WRITE access mode does not make sense from capability perspective.
    // We should revisit capability workflow if we want to support more modes.
    com.google.common.base.Preconditions.checkArgument(
        accessMode == alluxio.security.authorization.Mode.Bits.READ
            || accessMode == alluxio.security.authorization.Mode.Bits.WRITE,
        "only READ and WRITE access modes are supported");
    mAccessMode = accessMode;
    return this;
  }

  // ALLUXIO CS END
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetStatusOptions)) {
      return false;
    }
    GetStatusOptions that = (GetStatusOptions) o;
    return Objects.equal(mLoadMetadataType, that.mLoadMetadataType)
        // ALLUXIO CS ADD
        && Objects.equal(mAccessMode, that.mAccessMode)
        // ALLUXIO CS END
        && Objects.equal(mCommonOptions, that.mCommonOptions);
  }

  @Override
  public int hashCode() {
    // ALLUXIO CS REPLACE
    // return Objects.hashCode(mLoadMetadataType, mCommonOptions);
    // ALLUXIO CS WITH
    return Objects.hashCode(mLoadMetadataType, mCommonOptions, mAccessMode);
    // ALLUXIO CS END
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("loadMetadataType", mLoadMetadataType.toString())
        // ALLUXIO CS ADD
        .add("accessMode", mAccessMode.toString())
        // ALLUXIO CS END
        .toString();
  }
}
