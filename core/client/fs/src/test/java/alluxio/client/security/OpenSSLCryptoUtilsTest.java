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

package alluxio.client.security;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;

import org.junit.After;
import org.junit.Before;

/**
 * Unit tests for {@link CryptoUtils} with {@link OpenSSLCipher}.
 */
public final class OpenSSLCryptoUtilsTest extends CryptoUtilsTest {
  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.SECURITY_ENCRYPTION_OPENSSL_ENABLED, "true");
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

}
