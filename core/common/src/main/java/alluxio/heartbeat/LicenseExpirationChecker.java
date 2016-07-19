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

package alluxio.heartbeat;

import alluxio.LicenseUtils;

/**
 * A heartbeat executor to periodically check license expiration time.
 */
public final class LicenseExpirationChecker implements HeartbeatExecutor {
  /**
   * Constructs a new {@link LicenseExpirationChecker}.
   */
  public LicenseExpirationChecker() {}

  @Override
  public void heartbeat() {
    LicenseUtils.checkLicense();
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}
