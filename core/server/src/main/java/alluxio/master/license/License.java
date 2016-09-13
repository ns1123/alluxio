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

package alluxio.master.license;

import alluxio.Constants;
import alluxio.LicenseConstants;

import com.google.common.base.Objects;
import com.google.common.io.BaseEncoding;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Stores the license information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class License {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int BLOCK_SIZE = 16;
  private static final byte[] SECRET_KEY = LicenseConstants.LICENSE_SECRET_KEY.getBytes();
  private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

  private static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  private int mVersion;
  private String mName;
  private String mEmail;
  private String mKey;
  private String mChecksum;
  private String mSecret;
  private LicenseSecret mDecryptedSecret;

  /**
   * Creates a new instance of {@link License}.
   */
  public License() {}

  /**
   * @return the license format version
   */
  public int getVersion() {
    return mVersion;
  }

  /**
   * @return the license name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the license email
   */
  public String getEmail() {
    return mEmail;
  }

  /**
   * @return the license SECRET_KEY
   */
  public String getKey() {
    return mKey;
  }

  /**
   * @return the license checksum
   */
  public String getChecksum() {
    return mChecksum;
  }

  /**
   * @return the license secret
   */
  public String getSecret() {
    return mSecret;
  }

  /**
   * @return the license expiration (in milliseconds)
   */
  public long getExpiration() {
    return mDecryptedSecret.getExpiration();
  }

  /**
   * @return the license cluster size
   */
  public int getNodes() {
    return mDecryptedSecret.getNodes();
  }

  /**
   * @return whether the license is to be checked remotely
   */
  public boolean getRemote() {
    return mDecryptedSecret.getRemote();
  }

  /**
   * @return the license access token
   */
  public String getToken() {
    return mDecryptedSecret.getToken();
  }

  /**
   * @param version the version to use
   */
  public void setVersion(int version) {
    mVersion = version;
  }

  /**
   * @param name the name to use
   */
  public void setName(String name) {
    mName = name;
  }

  /**
   * @param email the email to use
   */
  public void setEmail(String email) {
    mEmail = email;
  }

  /**
   * @param key the SECRET_KEY to use
   */
  public void setKey(String key) {
    mKey = key;
  }

  /**
   * @param checksum the checksum to use
   */
  public void setChecksum(String checksum) {
    mChecksum = checksum;
  }

  /**
   * @param secret the SECRET_KEY to use
   */
  public void setSecret(String secret) {
    mSecret = secret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof License)) {
      return false;
    }
    License that = (License) o;
    return mVersion == that.mVersion && mName.equals(that.mName) && mEmail.equals(that.mEmail)
        && mKey.equals(that.mKey) && mChecksum.equals(that.mChecksum) && mSecret
        .equals(that.mSecret);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mVersion, mName, mEmail, mKey, mChecksum, mSecret);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("version", mVersion).add("name", mName)
        .add("email", mEmail).add("SECRET_KEY", mKey).add("checksum", mChecksum)
        .add("secret", mSecret).toString();
  }

  /**
   * Validates the integrity of the license.
   *
   * WARNING: This logic needs to match the logic used to generate the checksum
   *
   * @return whether the license information matches its checksum
   */
  public boolean validate() {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      return mChecksum
          .equals(bytesToHex(md.digest((mName + mEmail + mKey + mSecret + mVersion).getBytes())));
    } catch (Exception e) {
      LOG.error("Failed to generate checksum", e);
    }
    return false;
  }

  /**
   * Decrypts the license secret information.
   *
   * WARNING: This logic needs to match the logic used for encrypting the license secret.
   */
  void decryptSecret() throws GeneralSecurityException, IOException {
    byte[] text = BaseEncoding.base64().decode(mSecret);
    if (text.length < BLOCK_SIZE) {
      LOG.error("Secret too short");
    }
    byte[] iv = Arrays.copyOfRange(text, 0, BLOCK_SIZE);
    IvParameterSpec ivSpec = new IvParameterSpec(iv);
    text = Arrays.copyOfRange(text, BLOCK_SIZE, text.length);
    SecretKeySpec skeySpec = new SecretKeySpec(SECRET_KEY, "AES");
    Cipher cipher = Cipher.getInstance("AES/CFB/NoPadding");
    cipher.init(Cipher.DECRYPT_MODE, skeySpec, ivSpec);
    byte[] result = cipher.doFinal(text);
    ObjectMapper mapper = new ObjectMapper();
    mDecryptedSecret = mapper.readValue(result, LicenseSecret.class);
  }
}
