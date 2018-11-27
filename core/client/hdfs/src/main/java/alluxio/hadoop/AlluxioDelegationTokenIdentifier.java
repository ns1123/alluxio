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

package alluxio.hadoop;

import alluxio.security.authentication.DelegationTokenIdentifier;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * HDFS delegation token identifier for Alluxio service, wrapped around an Alluxio delegation token
 * identifier.
 */
public class AlluxioDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
  public static final Text ALLUXIO_DELEGATION_KIND = new Text("ALLUXIO_DELEGATION_TOKEN");
  private DelegationTokenIdentifier mAlluxioTokenId;

  /**
   * Default constructor for used with service loader.
   */
  public AlluxioDelegationTokenIdentifier() {
    // default initialization for deserializing from data later
  }

  /**
   * Creates an HDFS delegation token identifier based on Alluxio delegation token identifier.
   *
   * @param alluxioTokenId the Alluxio delegation token identifier
   */
  public AlluxioDelegationTokenIdentifier(DelegationTokenIdentifier alluxioTokenId) {
    super(new Text(Preconditions.checkNotNull(alluxioTokenId, "alluxioTokenId").getOwner()),
        new Text(alluxioTokenId.getRenewer()),
        new Text(alluxioTokenId.getRealUser()));
    mAlluxioTokenId = alluxioTokenId;
    setMaxDate(mAlluxioTokenId.getMaxDate());
    setIssueDate(mAlluxioTokenId.getIssueDate());
    // Token ids and key ids are int in Hadoop API. Converting them from long will lose some
    // information. Those ids are not meant to be used by Hadoop client, even if user uses it
    // somehow, it will take 68 years to overflow if one token is generated per second.
    setMasterKeyId((int) mAlluxioTokenId.getMasterKeyId());
    setSequenceNumber((int) mAlluxioTokenId.getSequenceNumber());
  }

  /**
   * @return the Alluxio delegation token identifier
   */
  public DelegationTokenIdentifier getAlluxioIdentifier() {
    return mAlluxioTokenId;
  }

  @Override
  public Text getKind() {
    return ALLUXIO_DELEGATION_KIND;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof AlluxioDelegationTokenIdentifier) {
      AlluxioDelegationTokenIdentifier that = (AlluxioDelegationTokenIdentifier) obj;
      return Objects.equal(mAlluxioTokenId, that.mAlluxioTokenId);
    }
    return false;
  }

  @Override
  public String toString() {
    return mAlluxioTokenId.toString();
  }

  @Override
  public int hashCode() {
    return mAlluxioTokenId.hashCode();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    byte[] buffer = WritableUtils.readCompressedByteArray(in);
    mAlluxioTokenId = DelegationTokenIdentifier.fromByteArray(buffer);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Preconditions.checkNotNull(mAlluxioTokenId, "mAlluxioTokenId");
    super.write(out);
    WritableUtils.writeCompressedByteArray(out, mAlluxioTokenId.getBytes());
  }
}
