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
  private String mToken;

  /**
   * Creates a new instance of {@link LicenseSecret}.
   */
  public LicenseSecret() {}

  /**
   * @return the license access token
   */
  public String getToken() {
    return mToken;
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
    return mToken.equals(that.mToken);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mToken);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("token", mToken).toString();
  }

}
