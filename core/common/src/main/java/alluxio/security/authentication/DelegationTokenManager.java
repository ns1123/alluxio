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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.security.MasterKey;
import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The class that manages delegation tokens on Alluxio master.
 */
public class DelegationTokenManager extends MasterKeyManager {
  private static final Logger LOG = LoggerFactory.getLogger(DelegationTokenManager.class);

  /** The maximum amount of time a delegation token. */
  private final long mMaxLifetime;
  /** The amount of time before which a delegation token must be renewed. */
  private final long mRenewInterval;
  /** Valid master keys. */
  private final Map<Long, MasterKey> mMasterKeys = new ConcurrentHashMap<>();
  /** Valid delegation tokens. */
  private Map<DelegationTokenIdentifier, TokenInfo> mTokens = new ConcurrentHashMap<>();
  /** Sequence number for creating delegation tokens. */
  private long mTokenSequenceNumber = 0L;

  /**
   * Creates a new {@link DelegationTokenManager} with parameters.
   *
   * @param keyUpdateIntervalMs the update interval of master key in millisecond
   * @param renewIntervalMs the amount of time a token can stay valid without renewal
   * @param maxLifetimeMs the maximum lifetime for the token
   */
  public DelegationTokenManager(long keyUpdateIntervalMs, long maxLifetimeMs, long renewIntervalMs) {
    super(keyUpdateIntervalMs + maxLifetimeMs, keyUpdateIntervalMs);
    mMaxLifetime = maxLifetimeMs;
    mRenewInterval = renewIntervalMs;
    // TODO(feng): load valid keys from journal before starting the thread pool
    startThreadPool();
  }

  /**
   * Creates a new {@link DelegationTokenManager} based on configuration.
   */
  public DelegationTokenManager() {
    this(Configuration.getMs(PropertyKey.SECURITY_AUTHENTICATION_DELEGATION_TOKEN_KEY_LIFETIME_MS),
        Configuration.getMs(PropertyKey.SECURITY_AUTHENTICATION_DELEGATION_TOKEN_LIFETIME_MS),
        Configuration.getMs(PropertyKey.SECURITY_AUTHENTICATION_DELEGATION_TOKEN_RENEW_INTERVAL_MS));
  }

  /**
   * Gets the delegation token for the given identifier.
   *
   * @param id the token identifier
   * @return the token
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(DelegationTokenIdentifier id) {
    id.setIssueDate(CommonUtils.getCurrentMs());
    id.setMaxDate(id.getIssueDate() + mMaxLifetime);
    id.setMasterKeyId(mMasterKey.getKeyId());
    id.setSequenceNumber(getNextTokenSequenceNumber());
    Token<DelegationTokenIdentifier> token = new Token<>(id, mMasterKey);
    mTokens.put(token.getId(),
        new TokenInfo(token, CommonUtils.getCurrentMs() + mRenewInterval));
    // TODO(feng): journal token updates.
    return token;
  }

  /**
   * Retrieves a password given the delegation token identifier.
   *
   * @param id the identifier
   * @return the password of the token if exists; otherwise null
   */
  public byte[] retrievePassword(DelegationTokenIdentifier id) {
    TokenInfo token = mTokens.get(id);
    if (token != null && token.getRenewDate() > CommonUtils.getCurrentMs()) {
      return token.getPassword();
    }
    return null;
  }

  // TODO(feng): Add delegation token renew/cancel logic

  private void swapMasterKeys() {
    try {
      mMasterKey = generateMasterKey();
      mMasterKeys.put(mMasterKey.getKeyId(), mMasterKey);
      LOG.debug("Master key {} is created.", mMasterKey.getKeyId());
      // TODO(feng): journal master key updates.
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  protected void maybeRotateAndDistributeKey() {
    swapMasterKeys();
    removeExpiredKeys();
    removeExpiredTokens();
  }

  private void removeExpiredKeys() {
    for (Iterator<Map.Entry<Long, MasterKey>> it = mMasterKeys.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<Long, MasterKey> entry = it.next();
      if (CommonUtils.getCurrentMs() > entry.getValue().getExpirationTimeMs()) {
        LOG.debug("Master key {} is expired and removed.", entry.getKey());
        it.remove();
        // TODO(feng): journal master key updates.
      }
    }
  }

  private void removeExpiredTokens() {
    for (Iterator<Map.Entry<DelegationTokenIdentifier, TokenInfo>> it =
         mTokens.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<DelegationTokenIdentifier, TokenInfo> entry = it.next();
      if (CommonUtils.getCurrentMs() > entry.getValue().getRenewDate()) {
        LOG.debug("Token {} is expired and removed.", entry.getKey().getSequenceNumber());
        it.remove();
        // TODO(feng): journal delegation token updates.
      }
    }
  }

  private synchronized long getNextTokenSequenceNumber() {
    return ++mTokenSequenceNumber;
  }

  /**
   * Private information of a token.
   */
  private class TokenInfo {
    private final byte[] mPassword;
    private final long mRenewDate;

    public TokenInfo(Token<DelegationTokenIdentifier> token, long renewDate) {
      mPassword = token.getPassword();
      mRenewDate = renewDate;
    }

    /**
     * @return password of the token
     */
    public byte[] getPassword() {
      return mPassword;
    }

    /**
     * @return renew date of the token
     */
    public long getRenewDate() {
      return mRenewDate;
    }
  }
}
