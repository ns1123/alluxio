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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateException;

import javax.annotation.concurrent.ThreadSafe;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

/**
 * Utility methods for working with Netty TLS.
 */
@ThreadSafe
public final class SSLUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SSLUtils.class);

  private static final String ALGORITHM = "SunX509";
  private static final String STORE_TYPE = "JKS";
  /** FQDN for self-signed SSL cert. */
  private static final String FQDN_NAME = "ALLUXIO.COM";

  // Static members for SSL contexts.
  private static SslContext sClientContext = null;
  private static SslContext sServerContext = null;
  // Static members for self-signed SSL contexts.
  private static SslContext sClientContextSelfSigned = null;
  private static SslContext sServerContextSelfSigned = null;

  private SSLUtils() {}

  /**
   * @param conf Alluxio configuration
   * @return Singleton Ssl context for server
   */
  public static SslContext getServerSslContext(AlluxioConfiguration conf) {
    if (sServerContext == null) {
      // Create server SSL context singleton.
      try {
        sServerContext = createServerSSLContext(conf);
      } catch (Exception e) {
        LOG.error("Failed to create server SSL context: {}", e);
      }
    }
    return sServerContext;
  }

  /**
   * @param conf Alluxio configuration
   * @return Singleton Ssl context for client
   */
  public static SslContext getClientSslContext(AlluxioConfiguration conf) {
    if (sClientContext == null) {
      // Create client SSL context singleton.
      try {
        sClientContext = createClientSSLContext(conf);
      } catch (Exception e) {
        LOG.error("Failed to create client SSL context: {}", e);
      }
    }
    return sClientContext;
  }

  /**
   * @return Singleton Ssl context for self-signed server
   */
  public static SslContext getSelfSignedServerSslContext() {
    if (sServerContextSelfSigned == null) {
      // Create self-signed server SSL context singleton.
      try {
        sServerContextSelfSigned = createSelfSignedServerContext();
      } catch (Exception e) {
        LOG.error("Failed to create self-signed server SSL context: {}", e);
      }
    }
    return sServerContextSelfSigned;
  }

  /**
   * @return Singleton Ssl context for self-signed client
   */
  public static SslContext getSelfSignedClientSslContext() {
    if (sClientContextSelfSigned == null) {
      // Create self-signed client SSL context singleton.
      try {
        sClientContextSelfSigned = createSelfSignedClientContext();
      } catch (Exception e) {
        LOG.error("Failed to create self-signed client SSL context: {}", e);
      }
    }
    return sClientContextSelfSigned;
  }

  /**
   * @param conf Alluxio configuration
   * @return SSLContext for the server side of an SSL connection
   */
  public static SslContext createServerSSLContext(AlluxioConfiguration conf) throws IOException {
    if (!conf.isSet(PropertyKey.NETWORK_TLS_KEYSTORE_PATH)) {
      throw new IOException("Failed to create server SSLContext. Config must be set: "
          + PropertyKey.NETWORK_TLS_KEYSTORE_PATH.getName());
    }
    if (!conf.isSet(PropertyKey.NETWORK_TLS_KEYSTORE_KEY_PASSWORD)) {
      throw new IOException("Failed to create server SSLContext. Config must be set: "
          + PropertyKey.NETWORK_TLS_KEYSTORE_KEY_PASSWORD.getName());
    }
    String keystore = conf.get(PropertyKey.NETWORK_TLS_KEYSTORE_PATH);
    String storePassword =
        conf.getOrDefault(PropertyKey.NETWORK_TLS_KEYSTORE_PASSWORD, null);
    String keyPassword = conf.get(PropertyKey.NETWORK_TLS_KEYSTORE_KEY_PASSWORD);

    try (InputStream inputStream = new FileInputStream(keystore)) {
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(ALGORITHM);
      KeyStore ks = KeyStore.getInstance(STORE_TYPE);
      ks.load(inputStream, storePassword != null ? storePassword.toCharArray() : null);
      kmf.init(ks, keyPassword.toCharArray());

      return SslContextBuilder.forServer(kmf)
          .applicationProtocolConfig(
              new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
                  ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                  ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                  ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1))
          .build();
    } catch (Exception e) {
      throw new IOException("Failed to create server SslContext.", e);
    }
  }

  /**
   * @param conf Alluxio configuration
   * @return SSLContext for the client side of an SSL connection
   */
  public static SslContext createClientSSLContext(AlluxioConfiguration conf) throws IOException {
    if (!conf.isSet(PropertyKey.NETWORK_TLS_TRUSTSTORE_PATH)) {
      throw new IOException("Failed to create client SSLContext. Config must be set: "
          + PropertyKey.NETWORK_TLS_TRUSTSTORE_PATH);
    }
    String truststore = conf.get(PropertyKey.NETWORK_TLS_TRUSTSTORE_PATH);
    String trustPassword =
        conf.getOrDefault(PropertyKey.NETWORK_TLS_TRUSTSTORE_PASSWORD, null);

    try (InputStream inputStream = new FileInputStream(truststore)) {

      TrustManagerFactory tmf = TrustManagerFactory.getInstance(ALGORITHM);
      KeyStore ks = KeyStore.getInstance(STORE_TYPE);
      ks.load(inputStream, trustPassword != null ? trustPassword.toCharArray() : null);
      tmf.init(ks);

      return SslContextBuilder.forClient().trustManager(tmf)
          .applicationProtocolConfig(
              new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
                  ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                  ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                  ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1))
          .build();
    } catch (Exception e) {
      throw new IOException("Failed to create client SslContext.", e);
    }
  }

  /**
   * @return SslContext for server side of an SSL connection with self-signed certificate
   * @throws CertificateException
   * @throws SSLException
   */
  private static SslContext createSelfSignedServerContext()
      throws CertificateException, SSLException {
    final SelfSignedCertificate ssc =
        new SelfSignedCertificate(FQDN_NAME, new SecureRandom(), 1024);
    // Create Ssl server context.
    return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
        .applicationProtocolConfig(
            new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1))
        .build();
  }

  /**
   * @return SslContext for client side of an SSL connection with self-signed certificate
   * @throws SSLException
   */
  private static SslContext createSelfSignedClientContext() throws SSLException {
    return SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE)
        .applicationProtocolConfig(
            new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1))
        .build();
  }
}
