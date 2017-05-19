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

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * The utils to communicate with TwoSigma Key Management Service (KMS).
 */
public final class TSKmsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TSKmsUtils.class);

  private static final String ENCRYPT_METHOD = "encrypt";
  private static final String DECRYPT_METHOD = "decrypt";
  private static final String KEY = "k";
  private static final String TTL = "ttl";
  private static final String DEFAULT_TTL_VALUE = "86400";
  private static final int RESPONSE_BUFFER_SIZE = 8 * Constants.KB;

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
    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpGet httpGet = new HttpGet(createURL(kms, op, params));
    CloseableHttpResponse response = httpclient.execute(httpGet);
    try {
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IOException("Request failed with: " + response.getStatusLine().toString());
      }
      return getCryptoKey(response);
    } finally {
      response.close();
    }
  }

  /**
   * @return The URL which is created
   */
  private static String createURL(String kms, String op, Map<String, String> params)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> parameter : params.entrySet()) {
      sb.append(parameter.getKey() + "=" + parameter.getValue() + "&");
    }
    return String.format("http://%s%s/%s?%s", kms, Constants.KMS_API_PREFIX, op, sb.toString());
  }

  /**
   * @param response the http response from KMS
   * @return the crypto key parsed from the InputStream of HttpURLConnection
   */
  private static EncryptionProto.CryptoKey getCryptoKey(CloseableHttpResponse response)
      throws IOException {
    InputStream inputStream = response.getEntity().getContent();
    byte[] buffer = new byte[RESPONSE_BUFFER_SIZE];
    int len = inputStream.read(buffer);
    EncryptionProto.CryptoKey key = EncryptionProto.CryptoKey.parseFrom(
        Arrays.copyOfRange(buffer, 0, len));
    return key;
  }

  private TSKmsUtils() {} // prevent instantiation
}
