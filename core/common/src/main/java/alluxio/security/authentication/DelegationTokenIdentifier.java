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

package alluxio.security.authentication;

import alluxio.proto.security.DelegationTokenProto;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.IOException;

/**
 * Identifier for delegation token used to authenticate a client user with master.
 */
public class DelegationTokenIdentifier implements TokenIdentifier {
  private final String mOwner;
  private final String mRenewer;
  private final String mRealUser;
  private long mIssueDate;
  private long mMaxDate;
  private long mSequenceNumber;
  private long mMasterKeyId = 0;

  /**
   * Constructs a {@link DelegationTokenIdentifier} with user information.
   * @param owner owner of the token
   * @param renewer user who can renew the token. If null is given, the token will not be renewable
   * @param realUser user who actually connected to the service
   */
  public DelegationTokenIdentifier(String owner, String renewer, String realUser) {
    mOwner = owner;
    mRenewer = renewer;
    mRealUser = realUser;
  }

  /**
   * Constructs a {@link DelegationTokenIdentifier}.
   * @param owner owner of the token
   * @param renewer user who can renew the token. If null is given, the token will not be renewable
   * @param realUser user who actually connected to the service
   * @param issueDate epoch time when the token is issued
   * @param maxDate epoch time when the token can be renewed until
   * @param sequenceNumber a unique identifier for the token
   * @param masterKeyId id of the master key
   */
  public DelegationTokenIdentifier(String owner, String renewer, String realUser,
      long issueDate, long maxDate, long sequenceNumber, long masterKeyId) {
    this(owner, renewer, realUser);
    setIssueDate(issueDate);
    setMaxDate(maxDate);
    setSequenceNumber(sequenceNumber);
    setMasterKeyId(masterKeyId);
  }

  /**
   * @return owner of the token
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return user who can renew the token
   */
  public String getRenewer() {
    return mRenewer;
  }

  /**
   * @return user who actually connected to the service
   */
  public String getRealUser() {
    return mRealUser;
  }

  /**
   * @return epoch time when the token is issued
   */
  public long getIssueDate() {
    return mIssueDate;
  }

  /**
   * Sets the epoch time when the token is issued.
   *
   * @param issueDate time when the token is issued
   */
  public void setIssueDate(long issueDate) {
    mIssueDate = issueDate;
  }

  /**
   * @return epoch time when the token can be renewed until
   */
  public long getMaxDate() {
    return mMaxDate;
  }

  /**
   * Sets the epoch time when the token can be renewed until.
   *
   * @param maxDate epoch time when the token can be renewed until
   */
  public void setMaxDate(long maxDate) {
    mMaxDate = maxDate;
  }

  /**
   * @return the unique identifier for the token
   */
  public long getSequenceNumber() {
    return mSequenceNumber;
  }

  /**
   * Sets the unique identifier for the token.
   *
   * @param sequenceNumber unique identifier for the token
   */
  public void setSequenceNumber(long sequenceNumber) {
    mSequenceNumber = sequenceNumber;
  }

  /**
   * @return id of the master key
   */
  public long getMasterKeyId() {
    return mMasterKeyId;
  }

  /**
   * Sets the id of the master key.
   *
   * @param masterKeyId id of the master key
   */
  public void setMasterKeyId(long masterKeyId) {
    mMasterKeyId = masterKeyId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("owner", mOwner)
        .add("renewer", mRenewer)
        .add("realUser", mRealUser)
        .add("issueDate", mIssueDate)
        .add("maxDate", mMaxDate)
        .add("sequenceNumber", mSequenceNumber)
        .add("masterKeyId", mMasterKeyId)
        .toString();
  }

  @Override
  public int hashCode() {
    return Long.hashCode(mSequenceNumber);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof DelegationTokenIdentifier)) {
      return false;
    }

    DelegationTokenIdentifier other = (DelegationTokenIdentifier) obj;
    return Objects.equal(mOwner, other.mOwner)
        && Objects.equal(mRenewer, other.mRenewer)
        && Objects.equal(mRealUser, other.mRealUser)
        && Objects.equal(mIssueDate, other.mIssueDate)
        && Objects.equal(mMaxDate, other.mMaxDate)
        && Objects.equal(mSequenceNumber, other.mSequenceNumber)
        && Objects.equal(mMasterKeyId, other.mMasterKeyId);
  }

  /**
   * @return the proto instance of the identifier
   */
  public DelegationTokenProto.DelegationTokenIdentifier toProto() {
    return DelegationTokenProto.DelegationTokenIdentifier.newBuilder()
        .setOwner(getOwner())
        .setRenewer(getRenewer())
        .setRealUser(getRealUser())
        .setIssueDate(getIssueDate())
        .setMaxDate(getMaxDate())
        .setSequenceNumber(getSequenceNumber())
        .setMasterKeyId(getMasterKeyId())
        .build();
  }

  @Override
  public byte[] getBytes() {
    return toProto().toByteArray();
  }

  /**
   * Converts a proto instance to the actual delegation token identifier.
   *
   * @param proto the proto delegation token identifier
   * @return the delegation token identifier
   */
  public static DelegationTokenIdentifier fromProto(
      DelegationTokenProto.DelegationTokenIdentifier proto) {
    return new DelegationTokenIdentifier(proto.getOwner(), proto.getRenewer(),
        proto.getRealUser(), proto.getIssueDate(), proto.getMaxDate(),
        proto.getSequenceNumber(), proto.getMasterKeyId());
  }

  /**
   * Deserializes data from byte array.
   *
   * @param data data to be deserialized
   * @return the delegation token identifier represented by the data
   * @throws IOException
   */
  public static DelegationTokenIdentifier fromByteArray(byte[] data)
      throws IOException {
    return fromProto(DelegationTokenProto.DelegationTokenIdentifier.parseFrom(data));
  }
}

