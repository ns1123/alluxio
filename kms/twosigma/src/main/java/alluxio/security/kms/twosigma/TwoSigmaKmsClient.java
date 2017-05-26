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

package alluxio.security.kms.twosigma;

import alluxio.Constants;
import alluxio.proto.security.EncryptionProto;
import alluxio.security.kms.KmsClient;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Two Sigma KMS client.
 */
public final class TwoSigmaKmsClient implements KmsClient {
  private static final String ENCRYPT_METHOD = "encrypt";
  private static final String DECRYPT_METHOD = "decrypt";
  private static final String KEY = "k";
  private static final String TTL = "ttl";
  private static final String DEFAULT_TTL_VALUE = "86400";
  private static final int RESPONSE_BUFFER_SIZE = 8 * Constants.KB;

  /**
   * Creates a new {@link TwoSigmaKmsClient}.
   */
  public TwoSigmaKmsClient() {}

  @Override
  public EncryptionProto.CryptoKey getCryptoKey(String kms, boolean encrypt, String inputKey)
      throws IOException {
    String op = encrypt ? ENCRYPT_METHOD : DECRYPT_METHOD;
    Map<String, String> params = new HashMap<>();
    params.put(KEY, inputKey);
    params.put(TTL, DEFAULT_TTL_VALUE);
    EncryptionProto.CryptoKey key = call(kms, op, params);
    return key;
  }

  private EncryptionProto.CryptoKey call(String kms, String op, Map<String, String> params)
      throws IOException {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpGet httpGet = new HttpGet(createURL(kms, op, params));
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IOException("Request failed with: " + response.getStatusLine().toString());
      }
      HttpEntity entity = response.getEntity();
      EncryptionProto.CryptoKey result = toCryptoKey(entity.getContent());
      EntityUtils.consume(entity);
      return result;
    }
  }

  /**
   * @return The URL which is created
   */
  private String createURL(String kms, String op, Map<String, String> params)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> parameter : params.entrySet()) {
      sb.append(parameter.getKey() + "=" + parameter.getValue() + "&");
    }
    return String.format("%s%s/%s?%s", kms, Constants.KMS_API_PREFIX, op, sb.toString());
  }

  /**
   * @param inputStream the response input stream
   * @return the crypto key parsed from the InputStream of HttpURLConnection
   */
  private EncryptionProto.CryptoKey toCryptoKey(InputStream inputStream)
      throws IOException {
    byte[] buffer = new byte[RESPONSE_BUFFER_SIZE];
    int len = inputStream.read(buffer);
    return EncryptionProto.CryptoKey.parseFrom(Arrays.copyOfRange(buffer, 0, len));
  }
}
