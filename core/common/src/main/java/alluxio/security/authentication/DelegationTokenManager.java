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
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.AccessControlException;
import alluxio.security.MasterKey;
import alluxio.util.CommonUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
  /** Valid master keys. Should only be updated by master or manager thread non-concurrently. */
  private final Map<Long, MasterKey> mMasterKeys = new ConcurrentHashMap<>();
  /** Valid delegation tokens. */
  private Map<DelegationTokenIdentifier, TokenInfo> mTokens = new ConcurrentHashMap<>();
  /** Listeners for manager triggered events. **/
  private final Set<EventListener> mEventListeners = new ConcurrentHashSet<>();
  /** Sequence number for creating delegation tokens. Should only be updated synchronously. */
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
    Preconditions.checkNotNull(id, "id");
    Preconditions.checkState(isRunning());
    MasterKey key = Preconditions.checkNotNull(mMasterKey, "mMasterKey");
    id.setIssueDate(CommonUtils.getCurrentMs());
    id.setMaxDate(id.getIssueDate() + mMaxLifetime);
    id.setMasterKeyId(key.getKeyId());
    // sequence number is unique given getNextTokenSequenceNumber() is synchronous
    id.setSequenceNumber(getNextTokenSequenceNumber());
    Token<DelegationTokenIdentifier> token = new Token<>(id, key);
    // no locking is required because token id is unique
    mTokens.put(token.getId(),
        new TokenInfo(token, CommonUtils.getCurrentMs() + mRenewInterval));
    return token;
  }

  /**
   * Retrieves a password given the delegation token identifier.
   *
   * @param id the identifier
   * @return the password of the token if exists; otherwise null
   */
  public byte[] retrievePassword(DelegationTokenIdentifier id) {
    Preconditions.checkNotNull(id, "id");
    TokenInfo token = mTokens.get(id);
    if (token != null && token.getRenewDate() > CommonUtils.getCurrentMs()) {
      return token.getPassword();
    }
    return null;
  }

  /**
   * Gets the renew time of a delegation token.
   *
   * @param id identifier of the token
   * @return the renew time
   */
  public long getDelegationTokenRenewTime(DelegationTokenIdentifier id) {
    Preconditions.checkNotNull(id, "id");
    if (!mTokens.containsKey(id))  {
      throw new IllegalArgumentException(
          String.format("Delegation token identifier not found: %s", id.toString()));
    }
    return mTokens.get(id).getRenewDate();
  }

  /**
   * Adds a delegation token. Should only be used before thread pool is started.
   *
   * @param id id of the delegation token
   * @param renewTime the renew time of the token
   */
  public void addDelegationToken(DelegationTokenIdentifier id, long renewTime) {
    Preconditions.checkNotNull(id, "id");
    Preconditions.checkState(!isRunning());
    // no locking required since no RPC or manager threads are running.
    if (id.getSequenceNumber() > mTokenSequenceNumber) {
      mTokenSequenceNumber = id.getSequenceNumber();
    }
    MasterKey masterKey = mMasterKeys.get(id.getMasterKeyId());
    if (masterKey == null) {
      throw new IllegalStateException(String.format("unable to find master key %d", masterKey.getKeyId()));
    }
    Token<DelegationTokenIdentifier> token = new Token<>(id, masterKey);
    mTokens.put(id, new TokenInfo(token, renewTime));
  }

  /**
   * Updates the manager to remove a delegation token. Should only be used before thread pool is started.
   *
   * @param id identifier of the token
   */
  public void updateDelegationTokenRemoval(DelegationTokenIdentifier id) {
    Preconditions.checkNotNull(id, "id");
    Preconditions.checkState(!isRunning());
    // no locking required since no RPC or manager threads are running.
    if (!mTokens.containsKey(id)) {
      LOG.warn("Failed to remove delegation token {}. The token does not exist.", id);
    }
    mTokens.remove(id);
  }

  /**
   * Updates a delegation token with new expiration time. Should only be used before thread pool is started.
   *
   * @param id identifier of the token
   * @param expirationTime new expiration time
   */
  public void updateDelegationTokenRenewal(DelegationTokenIdentifier id, long expirationTime) {
    Preconditions.checkNotNull(id, "id");
    Preconditions.checkState(!isRunning());
    // no locking required since no RPC or manager threads are running.
    if (!mTokens.containsKey(id)) {
      throw new IllegalStateException(String.format(
          "Failed to renew delegation token %s. The token does not exist.", id.toString()));
    }
    MasterKey masterKey = mMasterKeys.get(id.getMasterKeyId());
    if (masterKey == null) {
      throw new IllegalStateException(String.format("unable to find master key %d", masterKey.getKeyId()));
    }

    mTokens.put(id, new TokenInfo(new Token<>(id, masterKey), expirationTime));
  }

  /**
   * Renews a delegation token.
   *
   * @param token the token to renew
   * @param renewer the user who is trying to renew the delegation token
   * @return the new expiration epoch time
   */
  public synchronized long renewDelegationToken(Token<DelegationTokenIdentifier> token, String renewer)
      throws AccessControlException {
    Preconditions.checkNotNull(token, "token");
    Preconditions.checkNotNull(renewer, "renewer");
    DelegationTokenIdentifier id = token.getId();
    if (Strings.isNullOrEmpty(id.getRenewer())) {
      throw new AccessControlException(String.format(
          "token %s cannot be renewed because it does not have a user name set in the renewer field.",
          id.toString()));
    }
    if (!Objects.equal(id.getRenewer(), renewer)) {
      throw new AccessControlException(String.format(
          "user %s cannot renew a token with mismatched renewer %s", renewer, id.getRenewer()));
    }
    TokenInfo t = mTokens.get(id);
    if (t == null) {
      throw new IllegalArgumentException(
          String.format("failed to renew an unknown token %s", id.toString()));
    }
    if (CommonUtils.getCurrentMs() > id.getMaxDate()) {
      throw new IllegalArgumentException(
          String.format("failed to renew an expired token %s", id.toString()));
    }
    if (!Arrays.equals(t.getPassword(), token.getPassword())) {
      throw new AccessControlException(
          String.format("failed to renew token %s with invalid password", id.toString()));
    }
    long newExpirationTime = Math.min(
        token.getId().getMaxDate(),
        CommonUtils.getCurrentMs() + mRenewInterval);
    mTokens.put(id, new TokenInfo(token, newExpirationTime));
    return newExpirationTime;
  }

  /**
   * Cancels a delegation token.
   *
   * @param token the token to cancel
   * @param canceller the user who is trying to cancel the delegation token
   */
  public synchronized void cancelDelegationToken(Token<DelegationTokenIdentifier> token, String canceller)
      throws AccessControlException {
    Preconditions.checkNotNull(token, "token");
    Preconditions.checkNotNull(canceller, "canceller");
    DelegationTokenIdentifier id = token.getId();
    TokenInfo t = mTokens.get(id);
    if (t == null) {
      throw new IllegalArgumentException(
          String.format("failed to cancel an unknown token %s", id.toString()));
    }
    if (!Objects.equal(id.getOwner(), canceller)
        && (Strings.isNullOrEmpty(id.getRenewer()) || !Objects.equal(id.getRenewer(), canceller))) {
      throw new AccessControlException(String.format("user %s is not authorized to cancel the token %s.",
          canceller, id.toString()));
    }
    if (!Arrays.equals(t.getPassword(), token.getPassword())) {
      throw new AccessControlException(
          String.format("failed to cancel token %s with invalid password", id.toString()));
    }
    mTokens.remove(id);
  }

  /**
   * Adds a master key. Should only be used before thread pool is started.
   *
   * @param key the master key
   */
  public void addMasterKey(MasterKey key) {
    Preconditions.checkNotNull(key, "key");
    Preconditions.checkState(!isRunning());
    // no locking required since no RPC or manager threads are running.
    if (mMasterKeys.containsKey(key.getKeyId()))  {
      throw new IllegalArgumentException(String.format("master key %d already exists", key.getKeyId()));
    }
    mMasterKeys.put(key.getKeyId(), key);
    if (key.getKeyId() > mMasterKeyId) {
      mMasterKeyId = key.getKeyId();
    }
  }

  MasterKey getMasterKey(long keyId) {
    return mMasterKeys.get(keyId);
  }

  @Override
  protected void updateMasterKey() {
    try {
      MasterKey key = generateMasterKey();
      mMasterKeys.put(key.getKeyId(), key);
      LOG.debug("Master key {} is created.", key.getKeyId());
      mEventListeners.forEach(listener -> listener.onMasterKeyUpdated(key));
      mMasterKey = key;
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  protected void maybeRotateAndDistributeKey() {
    updateMasterKey();
    removeExpiredKeys();
    removeExpiredTokens();
  }

  private void removeExpiredKeys() {
    for (Iterator<Map.Entry<Long, MasterKey>> it = mMasterKeys.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<Long, MasterKey> entry = it.next();
      if (CommonUtils.getCurrentMs() > entry.getValue().getExpirationTimeMs()) {
        LOG.debug("Master key {} is expired and removed.", entry.getKey());
        it.remove();
      }
    }
  }

  private void removeExpiredTokens() {
    Set<DelegationTokenIdentifier> expiredIds = new HashSet<DelegationTokenIdentifier>();
    synchronized (this) {
      for (Iterator<Map.Entry<DelegationTokenIdentifier, TokenInfo>> it =
           mTokens.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<DelegationTokenIdentifier, TokenInfo> entry = it.next();
        if (CommonUtils.getCurrentMs() > entry.getValue().getRenewDate()) {
          LOG.debug("Token {} is expired and removed.", entry.getKey().getSequenceNumber());
          it.remove();
          expiredIds.add(entry.getKey());
        }
      }
    }
    // avoid synchronize on expensive event handlers
    expiredIds.forEach(id -> mEventListeners.forEach(listener -> listener.onDelegationTokenRemoved(id)));
  }

  private synchronized long getNextTokenSequenceNumber() {
    return ++mTokenSequenceNumber;
  }

  /**
   * Registers a event listener.
   *
   * @param listener listener to register
   */
  public void registerEventListener(EventListener listener) {
    mEventListeners.add(listener);
  }

  @Override
  public void reset() {
    // This should only be called after all RPC threads are stopped.
    super.reset();
    mMasterKeys.clear();
    mTokens.clear();
    mTokenSequenceNumber = 0L;
    LOG.debug("Delegation token manager is reset.");
  }

  /**
   * A listener for {@link DelegationTokenManager} events.
   */
  public interface EventListener {
    /**
     * Callback method triggered when a master key is updated.
     * Handler should avoid throwing exceptions unless fatal.
     *
     * @param key the new master key
     */
    void onMasterKeyUpdated(MasterKey key);

    /**
     * Callback method triggered when a delegation token is removed.
     * Handler should avoid throwing exceptions unless fatal.
     *
     * @param id identifier of the token
     */
    void onDelegationTokenRemoved(DelegationTokenIdentifier id);
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
