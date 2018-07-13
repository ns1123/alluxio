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

package alluxio.security.tls;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.network.thrift.ThriftUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

/**
 * Unit test for creating SSL sockets.
 */
public final class TLSSocketTest {
  private static final String KEYSTOREPASS = "keystorepass";
  private static final String KEYPASS = "keypass";
  private static final String TRUSTPASS = "trustpass";

  private String mKeystore;
  private String mTruststore;

  @Before
  public void before() throws Exception {
    createStores();
  }

  @Test
  public void thrift() throws Exception {
    int port = -1;

    // Create non-ssl server socket
    assertNotNull(ThriftUtils.createThriftServerSocket(
        (new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), 0))));

    // Create ssl server socket
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.NETWORK_TLS_ENABLED, "true",
        PropertyKey.NETWORK_TLS_KEYSTORE_PATH, mKeystore,
        PropertyKey.NETWORK_TLS_KEYSTORE_PASSWORD, KEYSTOREPASS,
        PropertyKey.NETWORK_TLS_KEYSTORE_KEY_PASSWORD, KEYPASS)).toResource()) {
      TServerSocket socket = ThriftUtils.createThriftServerSocket(
          (new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), 0)));
      assertTrue(socket.getServerSocket() instanceof SSLServerSocket);

      port = ThriftUtils.getThriftPort(socket);
    }

    assertNotEquals(-1, port);

    // Create non-ssl client socket
    assertNotNull(ThriftUtils
        .createThriftSocket((new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), port))));

    // Create ssl client socket
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.NETWORK_TLS_ENABLED, "true",
        PropertyKey.NETWORK_TLS_TRUSTSTORE_PATH, mTruststore,
        PropertyKey.NETWORK_TLS_TRUSTSTORE_PASSWORD, TRUSTPASS)).toResource()) {
      TSocket socket = ThriftUtils.createThriftSocket(
          (new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), port)));
      assertTrue(socket.getSocket() instanceof SSLSocket);
    }
  }

  private void createStores() throws Exception {
    File dir = AlluxioTestDirectory.createTemporaryDirectory("tls");

    mKeystore = new File(dir, "keystore.jks").getAbsolutePath();
    String cert = new File(dir, "selfsigned.cer").getAbsolutePath();
    mTruststore = new File(dir, "truststore.jks").getAbsolutePath();

    String args = String.format("cn=%s, ou=Department, o=Company, l=City, st=State, c=US",
        NetworkAddressUtils.getLocalHostName());

    // create keystore
    Process process = Runtime.getRuntime().exec(
        new String[] {"keytool", "-genkeypair", "-alias", "certificatekey", "-keyalg", "RSA",
            "-keysize", "2048", "-dname", args, "-keystore", mKeystore, "-keypass", KEYPASS,
            "-storepass", KEYSTOREPASS});
    try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      while (in.readLine() != null) {
      }
    }
    assertTrue(process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
    assertEquals(0, process.exitValue());

    // extract certificate
    process = Runtime.getRuntime().exec(
        new String[] {"keytool", "-export", "-alias", "certificatekey", "-keystore", mKeystore,
            "-storepass", KEYSTOREPASS, "-rfc", "-file", cert});
    try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      while (in.readLine() != null) {
      }
    }
    assertTrue(process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
    assertEquals(0, process.exitValue());

    // import certificate into truststore
    process = Runtime.getRuntime().exec(
        new String[] {"keytool", "-import", "-alias", "certificatekey", "-noprompt", "-file", cert,
            "-keystore", mTruststore, "-storepass", TRUSTPASS});
    try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      while (in.readLine() != null) {
      }
    }
    assertTrue(process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
    assertEquals(0, process.exitValue());
  }
}
