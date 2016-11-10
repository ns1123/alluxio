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

package alluxio.security.capability;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * The capability content has the information fields representing a short-lived user access
 * capability to a file.
 */
public final class CapabilityContent implements Serializable {

  private static final long serialVersionUID = -2559244844759627501L;

  /**
   * The access mode enum in capability. It can be either READ, WRITE or READ_WRITE.
   */
  public enum AccessMode {
    READ(1),
    WRITE(2),
    READ_WRITE(3),
    ;

    private final int mValue;

    AccessMode(int value) {
      mValue = value;
    }

    /**
     * @return the value of the accessMode
     */
    public int getValue() {
      return mValue;
    }

    /**
     * @return true if the accessMode has the write permission, false otherwise
     */
    public boolean hasWrite() {
      return mValue == 2 || mValue == 3;
    }

    /**
     * @return true if the accessMode has the read permission, false otherwise
     */
    public boolean hasRead() {
      return mValue == 1 || mValue == 3;
    }

    /**
     * Returns true if the current access mode includes a specified mode.
     *
     * @param am the specified access mode to check
     * @return true if the current access mode includes a specified mode, false otherwise
     */
    public boolean includes(AccessMode am) {
      return mValue == am.getValue() || mValue == 3;
    }

    /**
     * @return the string representative of the access mode
     */
    public String toString() {
      switch (mValue) {
        case 1:
          return "READ";
        case 2:
          return "WRITE";
        case 3:
          return "READ_WRITE";
        default:
          return "NOT_SUPPORTED";
      }
    }
  }

  // TODO(chaomin): add capability type for better extensiblity.
  private long mKeyId;
  private long mExpirationTimeMs;
  private String mOwner;
  private long mFileId;
  private AccessMode mAccessMode;

  /**
   * @return the default {@link CapabilityContent}
   */
  public static CapabilityContent defaults() {
    return new CapabilityContent();
  }

  /**
   * Default constructor.
   */
  private CapabilityContent() {
    mKeyId = 0L;
    mExpirationTimeMs = 0L;
    mOwner = null;
    mFileId = 0L;
    mAccessMode = null;
  }

  /**
   * @return the key id
   */
  public long getKeyId() {
    return mKeyId;
  }

  /**
   * @param keyId the capability key id to set
   * @return the updated object
   */
  public CapabilityContent setKeyId(long keyId) {
    mKeyId = keyId;
    return this;
  }

  /**
   * @return the expiration time
   */
  public long getExpirationTimeMs() {
    return mExpirationTimeMs;
  }

  /**
   * @param expirationTimeMs the expiration time to set in millisecond
   * @return the updated object
   */
  public CapabilityContent setExpirationTimeMs(long expirationTimeMs) {
    mExpirationTimeMs = expirationTimeMs;
    return this;
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @param owner the owner to set
   * @return the updated object
   */
  public CapabilityContent setOwner(String owner) {
    mOwner = owner;
    return this;
  }

  /**
   * @return the file id
   */
  public long getFileId() {
    return mFileId;
  }

  /**
   * @param fileId the file id to set
   * @return the updated object
   */
  public CapabilityContent setFileId(long fileId) {
    mFileId = fileId;
    return this;
  }

  /**
   * @return the access mode
   */
  public AccessMode getAccessMode() {
    return mAccessMode;
  }

  /**
   * @param accessMode the access mode to set
   * @return the updated object
   */
  public CapabilityContent setAccessMode(AccessMode accessMode) {
    mAccessMode = accessMode;
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mKeyId, mExpirationTimeMs, mOwner, mFileId, mAccessMode);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CapabilityContent)) {
      return false;
    }
    CapabilityContent that = (CapabilityContent) o;

    return Objects.equal(mKeyId, that.getKeyId())
        && Objects.equal(mExpirationTimeMs, that.getExpirationTimeMs())
        && Objects.equal(mOwner, that.getOwner())
        && Objects.equal(mKeyId, that.getFileId())
        && Objects.equal(mAccessMode, that.getAccessMode());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("keyId", mKeyId)
        .add("expirationTimeMs", mExpirationTimeMs)
        .add("owner", mOwner)
        .add("keyId", mKeyId)
        .add("mAccessMode", mAccessMode.toString())
        .toString();
  }
}
