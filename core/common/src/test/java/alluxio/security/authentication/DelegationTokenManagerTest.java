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

import alluxio.exception.AccessControlException;
import alluxio.security.MasterKey;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

/**
 * Unit tests for {@link DelegationTokenManager}.
 */
public class DelegationTokenManagerTest {
  private static final long KEY_UPDATE_INTERVAL_MS = 100L;
  private static final long TOKEN_RENEW_TIME_MS = 100L;
  private static final long TOKEN_MAX_LIFETIME_MS = 200L;
  private static final long TOKEN_LONG_MAX_LIFETIME_MS = 10000L;

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

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
  public void renewDelegationToken() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      Thread.sleep(10L);
      long expectedLifeTime = CommonUtils.getCurrentMs() + TOKEN_RENEW_TIME_MS;
      long newExpireTime = manager.renewDelegationToken(token, "renewer");

      Assert.assertTrue(newExpireTime >= expectedLifeTime);
    }
  }

  @Test
  public void renewDelegationTokenNotExceedMaxLifeTime() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_LONG_MAX_LIFETIME_MS * 2)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      Thread.sleep(10L);
      long newExpireTime = manager.renewDelegationToken(token, "renewer");

      Assert.assertEquals(token.getId().getMaxDate(), newExpireTime);
    }
  }

  @Test
  public void renewDelegationTokenMaxDateExceeded() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      manager.getDelegationTokenRenewTime(token.getId());
      Assert.assertEquals(id, token.getId());
      Thread.sleep(TOKEN_MAX_LIFETIME_MS + 50L);

      mExpectedException.expect(IllegalArgumentException.class);
      manager.renewDelegationToken(token, "renewer");

      Assert.fail("Renew a token with max life time exceeded should fail.");
    }
  }

  @Test
  public void renewDelegationTokenWrongPassword() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      Token<DelegationTokenIdentifier> badToken = new Token<>(token.getId(), "badpwd".getBytes());
      mExpectedException.expect(AccessControlException.class);
      manager.renewDelegationToken(badToken, "renewer");

      Assert.fail("Renew a token with wrong password should fail.");
    }
  }

  @Test
  public void renewDelegationTokenInvalidRenewer() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      mExpectedException.expect(AccessControlException.class);
      manager.renewDelegationToken(token, "invalid");

      Assert.fail("Renew a token with invalid renewer should fail.");
    }
  }

  @Test
  public void renewDelegationTokenNullRenewer() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      mExpectedException.expect(NullPointerException.class);
      manager.renewDelegationToken(token, null);

      Assert.fail("Renew a token with null renewer should fail.");
    }
  }

  @Test
  public void cancelDelegationToken() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      byte[] password = manager.retrievePassword(token.getId());
      Assert.assertNotNull(password);

      manager.cancelDelegationToken(token, "user");

      password = manager.retrievePassword(token.getId());
      Assert.assertNull(password);
    }
  }

  @Test
  public void cancelDelegationTokenWithRenewer() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      byte[] password = manager.retrievePassword(token.getId());
      Assert.assertNotNull(password);

      manager.cancelDelegationToken(token, "renewer");

      password = manager.retrievePassword(token.getId());
      Assert.assertNull(password);
    }
  }

  @Test
  public void cancelDelegationTokenWrongPassword() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      Token<DelegationTokenIdentifier> badToken = new Token<>(token.getId(), "badpwd".getBytes());
      mExpectedException.expect(AccessControlException.class);
      manager.cancelDelegationToken(badToken, "user");

      Assert.fail("Renew a token with wrong password should fail.");
    }
  }

  @Test
  public void cancelDelegationTokenWrongUser() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      mExpectedException.expect(AccessControlException.class);
      manager.cancelDelegationToken(token, "invalid");

      Assert.fail("Renew a token with wrong user should fail.");
    }
  }

  @Test
  public void cancelDelegationTokenNullUser() throws Exception {
    try (DelegationTokenManager manager = new DelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS)) {
      manager.startThreadPool();
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(id, token.getId());
      mExpectedException.expect(NullPointerException.class);
      manager.cancelDelegationToken(token, null);

      Assert.fail("Renew a token with null user should fail.");
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
      manager.updateDelegationTokenRemoval(token.getId());
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

  class TestDelegationTokenManager extends DelegationTokenManager {
    public TestDelegationTokenManager(long keyUpdateIntervalMs, long maxLifetimeMs, long renewIntervalMs) {
      super(keyUpdateIntervalMs, maxLifetimeMs, renewIntervalMs);
    }

    public void stopRotation() {
      mKeyRotationFuture.cancel(true);
    }

    public void rotate() {
      super.maybeRotateAndDistributeKey();
    }
  }

  @Test
  public void masterKeyExpiration() {
    try (TestDelegationTokenManager manager = new TestDelegationTokenManager(
        KEY_UPDATE_INTERVAL_MS, TOKEN_LONG_MAX_LIFETIME_MS, TOKEN_RENEW_TIME_MS) {
    }) {
      manager.startThreadPool();
      manager.stopRotation();
      long curKeyId = manager.getMasterKey().getKeyId();
      CommonUtils.sleepMs(KEY_UPDATE_INTERVAL_MS + 50L);
      DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy");
      // retrieves a delegation token after the key rotation interval passed but before the actual rotation
      Token<DelegationTokenIdentifier> token = manager.getDelegationToken(id);
      Assert.assertEquals(curKeyId, token.getId().getMasterKeyId());
      CommonUtils.sleepMs(50L);
      // simulates a late rotation
      manager.rotate();
      // verifies that the key expiration time is updated to be later than the token expiration time
      Assert.assertTrue(
          manager.getMasterKeys().get(curKeyId).getExpirationTimeMs() >= token.getId().getMaxDate());
    }
  }
}
