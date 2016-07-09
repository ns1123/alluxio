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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper function for AEE license validation.
 */
public final class LicenseUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Checks the license expiration time. If current timestamp exceeds the given license expiration
   * time, system exists. Otherwise does nothing.
   */
  public static void checkLicense() {
    if (System.currentTimeMillis() / 1000L > LicenseConstants.LICENSE_EXPIRATION) {
      LOG.error("######Stopping Alluxio service since the Alluxio license has expired.######");
      System.exit(-1);
    }
  }

  private LicenseUtils() {} // prevent instantiation
}
