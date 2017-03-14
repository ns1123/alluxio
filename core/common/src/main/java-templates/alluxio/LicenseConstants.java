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
 * Compile time license constants.
 */
public final class LicenseConstants {
  public static final String LICENSE_CHECK_ENABLED = "${license.check.enabled}";
  public static final String LICENSE_CHECK_PERIOD_MS = "${license.check.period}";
  public static final String LICENSE_GRACE_PERIOD_MS = "${license.grace.period}";
  public static final String LICENSE_SECRET_KEY = "${license.secret.key}";

  private LicenseConstants() {} // prevent instantiation
}
