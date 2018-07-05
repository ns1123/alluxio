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

package alluxio.util.network;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;

import javax.annotation.concurrent.ThreadSafe;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Utility methods for working with Netty TLS.
 */
@ThreadSafe
public final class SSLUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SSLUtils.class);
  private static final String PROTOCOL = "TLS";
  private static final String ALGORITHM = "SunX509";
  private static final String STORE_TYPE = "JKS";

  private SSLUtils() {}

  /**
   * @return SSLContext for the server side of an SSL connection
   */
  public static SSLContext createServerSSLContext() throws IOException {
    if (!Configuration.isSet(PropertyKey.NETWORK_TLS_KEYSTORE_PATH)) {
      throw new IOException("Failed to create server SSLContext. Config must be set: "
          + PropertyKey.NETWORK_TLS_KEYSTORE_PATH.getName());
    }
    if (!Configuration.isSet(PropertyKey.NETWORK_TLS_KEYSTORE_KEY_PASSWORD)) {
      throw new IOException("Failed to create server SSLContext. Config must be set: "
          + PropertyKey.NETWORK_TLS_KEYSTORE_KEY_PASSWORD.getName());
    }
    String keystore = Configuration.get(PropertyKey.NETWORK_TLS_KEYSTORE_PATH);
    String storePassword =
        Configuration.getOrDefault(PropertyKey.NETWORK_TLS_KEYSTORE_PASSWORD, null);
    String keyPassword = Configuration.get(PropertyKey.NETWORK_TLS_KEYSTORE_KEY_PASSWORD);

    try (InputStream inputStream = new FileInputStream(keystore)) {
      SSLContext ctx = SSLContext.getInstance(PROTOCOL);
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(ALGORITHM);
      KeyStore ks = KeyStore.getInstance(STORE_TYPE);
      ks.load(inputStream, storePassword != null ? storePassword.toCharArray() : null);
      kmf.init(ks, keyPassword.toCharArray());

      ctx.init(kmf.getKeyManagers(), null, null);
      return ctx;
    } catch (Exception e) {
      throw new IOException("Failed to create server SSLContext.", e);
    }
  }

  /**
   * @return SSLContext for the client side of an SSL connection
   */
  public static SSLContext createClientSSLContext() throws IOException {
    if (!Configuration.isSet(PropertyKey.NETWORK_TLS_TRUSTSTORE_PATH)) {
      throw new IOException("Failed to create client SSLContext. Config must be set: "
          + PropertyKey.NETWORK_TLS_TRUSTSTORE_PATH);
    }
    String truststore = Configuration.get(PropertyKey.NETWORK_TLS_TRUSTSTORE_PATH);
    String trustPassword =
        Configuration.getOrDefault(PropertyKey.NETWORK_TLS_TRUSTSTORE_PASSWORD, null);

    try (InputStream inputStream = new FileInputStream(truststore)) {
      SSLContext ctx = SSLContext.getInstance(PROTOCOL);
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(ALGORITHM);
      KeyStore ks = KeyStore.getInstance(STORE_TYPE);
      ks.load(inputStream, trustPassword != null ? trustPassword.toCharArray() : null);
      tmf.init(ks);

      ctx.init(null, tmf.getTrustManagers(), null);
      return ctx;
    } catch (Exception e) {
      throw new IOException("Failed to create client SSLContext.", e);
    }
  }
}
