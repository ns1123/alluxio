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
import alluxio.security.capability.SecretManager;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The class that manages master key on Alluxio master. It is used to generate and rotate
 * master keys on Alluxio master.
 */
public abstract class MasterKeyManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MasterKeyManager.class);
  /** The current master key. */
  protected volatile MasterKey mMasterKey;

  /** The update interval of master key. */
  private final long mKeyUpdateIntervalMs;
  /** The optional master key lifetime in millisecond, only used on master side. */
  protected final long mKeyLifetimeMs;
  /** The secret manager for generating master keys. */
  protected final SecretManager mSecretManager;
  /** The id for current master key. */
  protected long mMasterKeyId = 0L;

  protected ScheduledExecutorService mExecutor;
  private ScheduledFuture mKeyRotationFuture;

  /**
   * Creates a new {@link MasterKeyManager}.
   *
   * @param keyLifetimeMs the lifetime of master key in millisecond
   * @param keyUpdateIntervalMs the update interval of master key, in millisecond
   */
  public MasterKeyManager(long keyLifetimeMs, long keyUpdateIntervalMs) {
    mSecretManager = new SecretManager();
    mKeyLifetimeMs = keyLifetimeMs;
    mKeyUpdateIntervalMs = keyUpdateIntervalMs;
  }

  @Override
  public void close() {
    if (mKeyRotationFuture != null) {
      mKeyRotationFuture.cancel(true);
    }
    if (mExecutor != null) {
      mExecutor.shutdownNow();
    }
  }

  /**
   * @return the current {@link MasterKey}
   */
  public MasterKey getMasterKey() {
    return mMasterKey;
  }

  /**
   * @return the key lifetime in milliseconds
   */
  public long getKeyLifetimeMs() {
    return mKeyLifetimeMs;
  }

  /**
   * Starts the thread to rotate master key periodically.
   */
  protected void startThreadPool() {
    try {
      mMasterKey = generateMasterKey();
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      Throwables.propagate(e);
    }
    // TODO(chaomin): consider increase the number of threads for faster Alluxio start up
    // Some race conditions are avoided based on the assumption of single-thread pool here.
    mExecutor = new ScheduledThreadPoolExecutor(
        1, ThreadFactoryUtils.build(this.getClass().getSimpleName() + "-%d", true));
    mKeyRotationFuture =
        mExecutor.scheduleAtFixedRate(() -> maybeRotateAndDistributeKey(),
            mKeyUpdateIntervalMs, mKeyUpdateIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Generates a new master key.
   */
  protected MasterKey generateMasterKey() throws InvalidKeyException, NoSuchAlgorithmException {
    return new MasterKey(
        ++mMasterKeyId,
        CommonUtils.getCurrentMs() + mKeyLifetimeMs,
        mSecretManager.generateSecret().getEncoded());
  }

  /**
   * Rotates and distribute master key if necessary. It's called periodically by the scheduler.
   */
  protected abstract void maybeRotateAndDistributeKey();
}
