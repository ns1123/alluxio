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

package alluxio.security.authentication;

import alluxio.security.MasterKey;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Unit tests for {@link DelegationTokenManager}.
 */
public class DelegationTokenManagerTest {
  private static final long KEY_UPDATE_INTERVAL_MS = 100L;
  private static final long TOKEN_RENEW_TIME_MS = 100L;
  private static final long TOKEN_MAX_LIFETIME_MS = 200L;

  @Test
  public void getDelegationToken() {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
    }
  }

  @Test
  public void addDelegationToken() {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      long issueDate = CommonUtils.getCurrentMs();
      long seqNumber = 123L;
      DelegationTokenIdentifier id = new DelegationTokenIdentifier(
          "user", "renewer", "proxy", issueDate, issueDate + 30000L,
          seqNumber, manager.getMasterKey().getKeyId());
      Token<DelegationTokenIdentifier> token = new Token<>(id, manager.getMasterKey());
      long renewTime = CommonUtils.getCurrentMs() + 300000L;
      manager.close();
      manager.addDelegationToken(id, renewTime);
      manager.startThreadPool();

      byte[] password = manager.retrievePassword(id);
      Assert.assertArrayEquals(token.getPassword(), password);
      DelegationTokenIdentifier id2 = new DelegationTokenIdentifier(
          "user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token2 = manager.getDelegationToken(id2);
      Assert.assertTrue(token2.getId().getSequenceNumber() > seqNumber);
    }
  }

  @Test
  public void removeDelegationToken() {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      byte[] password = manager.retrievePassword(id);
      Assert.assertNotNull(password);
      manager.close();
      manager.removeDelegationToken(token.getId());
      manager.startThreadPool();

      password = manager.retrievePassword(id);
      Assert.assertNull(password);
    }
  }

  @Test
  public void addMasterKey() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      long keyId = 123L;
      MasterKey key = new MasterKey(keyId, CommonUtils.getCurrentMs(), null);
      manager.addMasterKey(key);

      Assert.assertEquals(key, manager.getMasterKey(keyId));

      manager.startThreadPool();
      Assert.assertTrue(manager.getMasterKey().getKeyId() > keyId);
    }
  }

  @Test
  public void retrievePasswordSuccess() {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      byte[] password = manager.retrievePassword(id);
      Assert.assertEquals(token.getPassword(), password);
    }
  }

  @Test
  public void retrievePasswordNotExist() {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      byte[] password = manager.retrievePassword(id);
      Assert.assertNull(password);
    }
  }

  @Test
  public void retrievePasswordExpired() {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy",
          0L, 1L, 2L, 3L);
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      byte[] password = manager.retrievePassword(id);
      Assert.assertNotNull(password);
      CommonUtils.sleepMs(TOKEN_RENEW_TIME_MS + 50L);
      password = manager.retrievePassword(id);
      Assert.assertNull(password);
    }
  }

  @Test
  public void keyRotation() {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      long curKeyId = manager.getMasterKey().getKeyId();
      byte[] oldSecretKey = manager.getMasterKey().getEncodedKey();
      CommonUtils.sleepMs(KEY_UPDATE_INTERVAL_MS + 50L);
      Assert.assertEquals(curKeyId + 1, manager.getMasterKey().getKeyId());
      byte[] newSecretKey = manager.getMasterKey().getEncodedKey();
      Assert.assertFalse(Arrays.equals(oldSecretKey, newSecretKey));
    }
  }
}
