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

import alluxio.Constants;
import alluxio.proto.security.EncryptionProto;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;

/**
 * The utils to communicate with TwoSigma Key Management Service (KMS).
 */
public final class TSKmsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TSKmsUtils.class);

  private static final String ENCRYPT_METHOD = "encrypt";
  private static final String DECRYPT_METHOD = "decrypt";
  private static final String KEY = "k";
  private static final String TTL = "tt";
  private static final String DEFAULT_TTL_VALUE = "86400";

  /**
   * Gets the crypto key from KMS.
   *
   * @param kms the KMS endpoint
   * @param encrypt whether encrypt or decrypt
   * @param encryptionKey the key for requesting crypto key
   * @return the retrieved crypto key
   */
  public static EncryptionProto.CryptoKey getCryptoKey(
      String kms, boolean encrypt, String encryptionKey) throws IOException {
    String op = encrypt ? ENCRYPT_METHOD : DECRYPT_METHOD;
    Map<String, String> params = new HashMap<>();
    params.put(KEY, encryptionKey);
    params.put(TTL, DEFAULT_TTL_VALUE);
    EncryptionProto.CryptoKey key = call(kms, op, params);
    LOG.debug("DEBUG CHAOMIN: {}", key);
    return key;
  }

  private static EncryptionProto.CryptoKey call(String kms, String op, Map<String, String> params)
      throws IOException {
    HttpURLConnection connection = (HttpURLConnection) createURL(kms, op, params).openConnection();
    connection.setRequestMethod("GET");
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/octet-stream");

    connection.connect();
    if (connection.getResponseCode() != Response.Status.OK.getStatusCode()) {
      InputStream errorStream = connection.getErrorStream();
      if (errorStream != null) {
        throw new IOException("Request failed: " + IOUtils.toString(errorStream));
      }
      throw new IOException("Request failed with status code " + connection.getResponseCode());
    }
    return getResponse(connection);
  }

  /**
   * @return The URL which is created
   */
  private static URL createURL(String kms, String op, Map<String, String> params)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> parameter : params.entrySet()) {
      sb.append(parameter.getKey() + "=" + parameter.getValue() + "&");
    }
    return new URL(
        "http://" + kms + Constants.KMS_API_PREFIX + "/" + op
            + "?" + sb.toString());
  }

  /**
   * @param connection the HttpURLConnection
   * @return the crypto key parsed from the InputStream of HttpURLConnection
   */
  private static EncryptionProto.CryptoKey getResponse(HttpURLConnection connection)
      throws IOException {
    InputStream inputStream = connection.getInputStream();
    byte[] buffer = new byte[8 * Constants.KB];
    int len = inputStream.read(buffer);
    EncryptionProto.CryptoKey key = EncryptionProto.CryptoKey.parseFrom(
        Arrays.copyOfRange(buffer, 0, len));
    return key;
  }

  private TSKmsUtils() {} // prevent instantiation
}
