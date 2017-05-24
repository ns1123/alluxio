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

package alluxio.worker.security;

import alluxio.Constants;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidCapabilityException;
import alluxio.exception.PreconditionMessage;
import alluxio.proto.security.CapabilityProto;
import alluxio.security.authorization.Mode;
import alluxio.security.capability.CapabilityKey;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class implements a threadsafe capability cache used by the block worker. The cache is
 * invalidated in the following two scenarios:
 * 1. The capability is expired.
 * 2. The client user disconnects from the worker.
 */
@ThreadSafe
public final class CapabilityCache implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CapabilityCache.class);

  private ConcurrentHashMapV8<String, Cache> mUserCache;
  private ScheduledExecutorService mExecutor;
  private ScheduledFuture mGcFuture;

  private ReentrantLock mCapabilityKeyLock = new ReentrantLock();
  @GuardedBy("mCapabilityKeyLock")
  private CapabilityKey mOldCapabilityKey;
  @GuardedBy("mCapabilityKeyLock")
  private CapabilityKey mCapabilityKey;

  /**
   * The options to create a {@link CapabilityCache}.
   */
  public static final class Options {
    public long mGCIntervalMs = Constants.MINUTE_MS;
    public ScheduledExecutorService mExecutorService;
    public CapabilityKey mCapabilityKey;

    /**
     * @return the default instance
     */
    public static Options defaults() {
      return new Options();
    }

    /**
     * @param gcIntervalMs the interval to GC expired cache entries
     * @return the updated options object
     */
    public Options setGCIntervalMs(long gcIntervalMs) {
      mGCIntervalMs = gcIntervalMs;
      return this;
    }

    /**
     * @param executor the executor used to run the garbage collector
     * @return the updated options object
     */
    public Options setExecutor(ScheduledExecutorService executor) {
      mExecutorService = executor;
      return this;
    }

    /**
     * @param key the capability key
     * @return the updated options object
     */
    public Options setCapabilityKey(CapabilityKey key) {
      mCapabilityKey = key;
      return this;
    }

    /**
     * Private default constructor.
     */
    private Options() {}
  }

  /**
   * Creates an instance of the {@link CapabilityCache}.
   *
   * @param options the options
   */
  public CapabilityCache(Options options) {
    mCapabilityKey = Preconditions.checkNotNull(options.mCapabilityKey);
    mUserCache = new ConcurrentHashMapV8<>();
    if (options.mExecutorService == null) {
      mExecutor =
          new ScheduledThreadPoolExecutor(1, ThreadFactoryUtils.build("CapabilityCache-%d", true));
    } else {
      mExecutor = options.mExecutorService;
    }
    mGcFuture = mExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        gc();
      }
    }, options.mGCIntervalMs, options.mGCIntervalMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    mGcFuture.cancel(true);
    mExecutor.shutdown();
    try {
      mExecutor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return the capability key
   */
  public CapabilityKey getCapabilityKey() {
    return mCapabilityKey;
  }

  /**
   * @param key the capability key
   */
  public void setCapabilityKey(CapabilityKey key) {
    mCapabilityKeyLock.lock();
    try {
      mOldCapabilityKey = mCapabilityKey;
      mCapabilityKey = key;
    } finally {
      mCapabilityKeyLock.unlock();
    }
  }

  /**
   * Adds capability represented in a thrift object to the cache.
   *
   * @param capability the capability in thrift form
   * @throws InvalidCapabilityException if the thrift representation of the capability is not valid
   */
  public void addCapability(alluxio.proto.security.CapabilityProto.Capability capability)
      throws InvalidCapabilityException {
    if (capability == null || !capability.hasKeyId()) {
      return;
    }
    alluxio.security.capability.Capability cap =
        new alluxio.security.capability.Capability(capability);
    CapabilityKey key;
    mCapabilityKeyLock.lock();
    try {
      if (cap.getKeyId() == mCapabilityKey.getKeyId()) {
        key = mCapabilityKey;
      } else if (mOldCapabilityKey != null && cap.getKeyId() == mOldCapabilityKey.getKeyId()) {
        key = mOldCapabilityKey;
      } else {
        throw new InvalidCapabilityException(String.format(
            "No matching capability key found. Expected key ID: %d. Current key ID: %d. Old key ID:"
                + " %d.", cap.getKeyId(), mCapabilityKey.getKeyId(),
            mOldCapabilityKey == null ? -1 : mOldCapabilityKey.getKeyId()));
      }
    } finally {
      mCapabilityKeyLock.unlock();
    }
    cap.verifyAuthenticator(key);
    addCapabilityInternal(cap.getContentDecoded());
  }

  /**
   * Clears the capability cache for a particular user.
   * This is only used in the test for now.
   *
   * @param user the user name
   */
  public void expireCapabilityForUser(String user) {
    Cache cache = mUserCache.get(user);
    Preconditions.checkNotNull(cache, PreconditionMessage.ERR_USER_NOT_SET.toString(), user);
    cache.mContents.clear();
  }

  /**
   * Checks whether a user has the requested access to a file.
   *
   * @param user the user
   * @param fileId the file Id
   * @param accessRequested the access requested
   * @throws AccessControlException if the user does not have the permission to access the file
   * @throws InvalidCapabilityException if the capability for this <user, fileId> pair is invalid
   */
  public void checkAccess(String user, long fileId, Mode.Bits accessRequested)
      throws AccessControlException, InvalidCapabilityException {
    Cache cache = mUserCache.get(user);
    Preconditions.checkNotNull(cache, PreconditionMessage.ERR_USER_NOT_SET.toString(), user);

    Cache.Content content = cache.mContents.get(fileId);
    if (content == null) {
      throw new InvalidCapabilityException(ExceptionMessage.CAPABILITY_EXPIRED.getMessage());
    }
    if (!content.mAccessMode.imply(accessRequested)) {
      throw new AccessControlException(String.format(
          "Permission denied. %s is not allowed to access fileId (%d) with access mode %s "
              + "(allowed mode: %s).", user, fileId, accessRequested, content.mAccessMode));
    }
  }

  /**
   * Increments the user connection count.
   *
   * @param user the current client user
   */
  public void incrementUserConnectionCount(String user) {
    while (true) {
      Cache cache = mUserCache.get(user);
      if (cache == null) {
        Cache newCache = new Cache();
        Cache oldCache = mUserCache.putIfAbsent(user, newCache);
        if (oldCache == null) {
          return;
        }
        cache = oldCache;
      }
      cache.mCountLock.lock();
      try {
        // There is a small chance that the cache is removed for the user before the lock
        // is acquired.
        if (cache.mConnectionCount > 0) {
          cache.mConnectionCount++;
          return;
        }
      } finally {
        cache.mCountLock.unlock();
      }
    }
  }

  /**
   * Decrements the user connection count. If the user's connection count becomes 0 after this
   * decrement, the {@link Cache} corresponding to the user is removed.
   *
   * @param user the current client user
   */
  // TODO(peis): Consider waiting sometime before invalidating the entry.
  public void decrementUserConnectionCount(String user) {
    Cache cache = mUserCache.get(user);
    Preconditions.checkNotNull(cache);
    cache.mCountLock.lock();
    try {
      Preconditions.checkState(cache.mConnectionCount > 0);
      cache.mConnectionCount--;
      if (cache.mConnectionCount == 0) {
        mUserCache.remove(user);
      }
    } finally {
      cache.mCountLock.unlock();
    }
  }

  /**
   * Adds a capability content to the cache.
   * NOTE: the caller must guarantee that the owner of the capability content has at least one
   * active connection to the worker.
   *
   * @param content the capability content
   */
  private void addCapabilityInternal(CapabilityProto.Content content) {
    if (CommonUtils.getCurrentMs() > content.getExpirationTimeMs()) {
      LOG.warn("The capability {} to add is expired before being added to cache.",
          content.toString());
      return;
    }

    Cache cache = mUserCache.get(content.getUser());
    Preconditions
        .checkNotNull(cache, PreconditionMessage.ERR_USER_NOT_SET.toString(), content.toString());
    cache.mContents.put(content.getFileId(), new Cache.Content(content));
  }

  /**
   * Garbage collects the expired cache entries for every user.
   */
  private void gc() {
    for (Cache cache : mUserCache.values()) {
      cache.gc();
    }
  }

  /**
   * The cache data structure per user.
   */
  private static final class Cache {
    /**
     * The cached capability content converted from {@link CapabilityProto.Content}.
     */
    private static final class Content {
      /** The access mode. */
      public Mode.Bits mAccessMode;
      /** The expire timestamp in milliseconds. */
      public long mExpireTimestampMs;

      /**
       * Creates an instance of {@link Content}.
       *
       * @param content the capability content in {@link CapabilityProto.Content}
       */
      public Content(CapabilityProto.Content content) {
        mAccessMode = Mode.Bits.fromShort((short) content.getAccessMode());
        mExpireTimestampMs = content.getExpirationTimeMs();
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof Content)) {
          return false;
        }
        Content other = (Content) o;
        return Objects.equal(mAccessMode, other.mAccessMode) && Objects
            .equal(mExpireTimestampMs, other.mExpireTimestampMs);
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(mAccessMode, mExpireTimestampMs);
      }
    }

    /** The cached capability contents. */
    public ConcurrentHashMapV8<Long, Content> mContents;

    public ReentrantLock mCountLock = new ReentrantLock();
    @GuardedBy("mCountLock")
    public int mConnectionCount;

    /**
     * Creates an instance of {@link Cache}.
     */
    public Cache() {
      mContents = new ConcurrentHashMapV8<>();
      mConnectionCount = 1;
    }

    /**
     * Garbage collects the expired cache entries.
     */
    public void gc() {
      Set<Map.Entry<Long, Content>> entrySet = mContents.entrySet();
      Iterator<Map.Entry<Long, Content>> iterator =  entrySet.iterator();
      long currentTimeMs = CommonUtils.getCurrentMs();
      while (iterator.hasNext()) {
        Map.Entry<Long, Content> entry = iterator.next();
        if (currentTimeMs > entry.getValue().mExpireTimestampMs) {
          mContents.remove(entry.getKey(), entry.getValue());
        }
      }
    }
  }
}
