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

package alluxio.master.license;

import com.google.common.base.Objects;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Representation of information encrypted in the license.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LicenseSecret {
  private long mExpirationMs;
  private int mNodes;
  private boolean mRemote;
  private String mToken;

  /**
   * Creates a new instance of {@link LicenseSecret}.
   */
  public LicenseSecret() {}

  /**
   * @return the license expiration (in milliseconds)
   */
  public long getExpiration() {
    return mExpirationMs;
  }

  /**
   * @return the maximum license cluster size
   */
  public int getNodes() {
    return mNodes;
  }

  /**
   * @return whether the license is to be checked remotely
   */
  public boolean getRemote() {
    return mRemote;
  }

  /**
   * @return the license access token
   */
  public String getToken() {
    return mToken;
  }

  /**
   * @param expirationMs the license expiration to use (in milliseconds)
   */
  public void setExpiration(long expirationMs) {
    mExpirationMs = expirationMs;
  }

  /**
   * @param nodes the maximum license cluster size to use
   */
  public void setNodes(int nodes) {
    mNodes = nodes;
  }

  /**
   * @param remote the license remote check value to use
   */
  public void setRemote(boolean remote) {
    mRemote = remote;
  }

  /**
   * @param token the license access token to use
   */
  public void setToken(String token) {
    mToken = token;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LicenseSecret)) {
      return false;
    }
    LicenseSecret that = (LicenseSecret) o;
    return mExpirationMs == that.mExpirationMs && mNodes == that.mNodes && mRemote == that.mRemote
        && mToken.equals(that.mToken);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mExpirationMs, mNodes, mRemote, mToken);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("expiration", mExpirationMs).add("nodes", mNodes)
        .add("remote", mRemote).add("token", mToken).toString();
  }

}
