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

package alluxio.master.security.capability;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnavailableException;
import alluxio.master.block.BlockMaster;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.security.MasterKey;
import alluxio.security.authentication.MasterKeyManager;
import alluxio.util.IdUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

/**
 * The class that manages master key for generating capability on Alluxio master.
 */
public class CapabilityKeyManager extends MasterKeyManager {
  private static final Logger LOG = LoggerFactory.getLogger(CapabilityKeyManager.class);

  private static final long KEY_DISTRIBUTION_RETRY_INTERVAL_MS = 100L;

  /** The new capability key being sent to workers, for key rotation. */
  // TODO(chaomin): if the single-thread executor assumption is changed, should add locking for it.
  private MasterKey mNewKey;
  /** The block worker contains the list of active worker. */
  private final BlockMaster mBlockMaster;

  public ReentrantLock mCountLock = new ReentrantLock();
  @GuardedBy("mCountLock")
  public int mActiveKeyUpdateCount;

  /**
   * Creates a new {@link CapabilityKeyManager}.
   *
   * @param keyLifetimeMs the lifetime of key in millisecond
   * @param blockMaster the block master
   */
  public CapabilityKeyManager(long keyLifetimeMs, BlockMaster blockMaster) {
    super(keyLifetimeMs, keyLifetimeMs * 3L / 4L);
    mBlockMaster = blockMaster;
    mMasterKeyId = IdUtils.getRandomNonNegativeLong() % Integer.MAX_VALUE + 1L;
    startThreadPool();
  }

  /**
   * Prepares the new capability key, which will be sent to workers.
   */
  private void prepareNewKey() {
    try {
      mNewKey = generateMasterKey();
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Schedules a new key distribution to the given worker.
   *
   * @param worker the target worker info
   */
  public void scheduleNewKeyDistribution(WorkerInfo worker) {
    // Increment the active connection counter by 1 at the start of key distribution.
    incrementActiveKeyUpdateCount();
    mExecutor.submit(createDistributeKeyRunnable(worker));
    LOG.debug("New key distribution is scheduled for worker {}", worker.getAddress());
  }

  /**
   * Creates a new {@link Runnable} for distributing a key to the given worker.
   *
   * @param worker the target worker
   * @return the created runnable
   */
  private Runnable createDistributeKeyRunnable(final WorkerInfo worker) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          distributeKey(worker);
        } catch (Exception e) {
          LOG.error("Capability key distribution failed to worker {}", worker.getAddress());
        }
      }
    };
  }

  /**
   * Distributes the current key to a given worker. Retries if the capability key transfer fails,
   * until the worker is identified as disconnected.
   *
   * @param worker the target worker info
   */
  private void distributeKey(WorkerInfo worker) {
    Set<Long> workerIds = new HashSet<>();
    List<WorkerInfo> workerInfos = getWorkerInfoList();
    for (WorkerInfo workerInfo : workerInfos) {
      workerIds.add(workerInfo.getId());
    }
    if (!workerIds.contains(worker.getId())) {
      // The worker is no longer connected, decrease the active connection by 1 and check
      // whether all connections are finished.
      LOG.warn(
          "Worker {} is lost before distributing the new capability key. The current list of "
              + "workers are {}.", worker, workerInfos);
      decrementActiveKeyUpdateCount();
      return;
    }

    // mNewKey is null when there is no key rotation ongoing, thus send the current key.
    // Otherwise the new key is being prepared, always send the new key.
    MasterKey key = mNewKey == null ? mMasterKey : mNewKey;
    try {
      LOG.debug("Sending key with id {} to worker {}", key.getKeyId(), worker.getAddress());
      // TODO(ggezer) EE-SEC Make the call on new secure gRPC server.
      //NettySecretKeyWriter
      //    .write(NetworkAddressUtils.getSecureRpcPortSocketAddress(worker.getAddress()), key);
      throw new IOException("not implemented");
    } catch (IOException e) {
      LOG.debug("Retrying to send key with id {} to worker {}, previously failed with: {}",
          key.getKeyId(), worker.getAddress(), e.getMessage());
      incrementActiveKeyUpdateCount();
      mExecutor.schedule(createDistributeKeyRunnable(worker), KEY_DISTRIBUTION_RETRY_INTERVAL_MS,
          TimeUnit.MILLISECONDS);
    }

    // The key distribution finishes, decrease the active connection by 1 and check whether all
    // connections are finished.
    decrementActiveKeyUpdateCount();
  }

  /**
   * Increments the active key update counter by 1.
   */
  private void incrementActiveKeyUpdateCount() {
    mCountLock.lock();
    mActiveKeyUpdateCount++;
    mCountLock.unlock();
  }

  /**
   * Decrements the active key update counter by 1 and checks whether all connections are finished.
   * If all connections are finished, safely update the capability key with the new one.
   */
  private void decrementActiveKeyUpdateCount() {
    mCountLock.lock();
    mActiveKeyUpdateCount--;
    if (mActiveKeyUpdateCount == 0 && mNewKey != null) {
      mMasterKey = mNewKey;
      mNewKey = null;
    }
    mCountLock.unlock();
  }

  /**
   * Prepares a new key and distributes it to all workers when there's no pending active update.
   */
  @Override
  protected void maybeRotateAndDistributeKey() {
    mCountLock.lock();
    boolean hasPendingUpdate = mActiveKeyUpdateCount > 0;
    mCountLock.unlock();

    // If there're pending key updates, defer the key rotation until the pending ones are cleared.
    if (hasPendingUpdate) {
      mExecutor.schedule(new Runnable() {
        @Override
        public void run() {
          maybeRotateAndDistributeKey();
        }
      }, KEY_DISTRIBUTION_RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
      return;
    }

    prepareNewKey();
    List<WorkerInfo> workerInfoList = getWorkerInfoList();
    for (WorkerInfo worker : workerInfoList) {
      scheduleNewKeyDistribution(worker);
    }

    // This should only never happen in production.
    if (workerInfoList.isEmpty() && mNewKey != null) {
      mCountLock.lock();
      if (mActiveKeyUpdateCount == 0) {
        mMasterKey = mNewKey;
        mNewKey = null;
      }
      mCountLock.unlock();
    }
  }

  /**
   * @return a list of workers (with retries)
   */
  private List<WorkerInfo> getWorkerInfoList() {
    long wait = ServerConfiguration
        .getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME) + 10 * Constants.SECOND_MS;
    RetryPolicy retry = new TimeoutRetry(wait, Constants.SECOND_MS);
    while (retry.attempt()) {
      try {
        return mBlockMaster.getWorkerInfoList();
      } catch (UnavailableException e) {
        // This only happens when the server is in safe mode (a temporary state).
        if (retry.getAttemptCount() == 1) {
          LOG.warn("Failed to query the list of workers, retrying. error: {}", e.getMessage());
        }
      }
    }
    LOG.warn("Failed to get list of workers after retrying for {}ms.", wait);
    return Collections.emptyList();
  }
}
