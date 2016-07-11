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

package alluxio;

/**
 * License constants from compilation time for AEE license validation.
 */
public final class LicenseConstants {
  /* License expiration date in yyyy-MM-dd, specified in maven property. **/
  public static final String LICENSE_EXPIRATION_DATE = "${license.expiration.date}";

  private LicenseConstants() {} // prevent instantiation
}
