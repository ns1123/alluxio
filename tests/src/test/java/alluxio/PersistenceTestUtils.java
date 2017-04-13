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

package alluxio;

import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.PersistJob;
import alluxio.util.CommonUtils;

import com.google.common.base.Function;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Util methods for persistence integration tests.
 */
@NotThreadSafe
public final class PersistenceTestUtils {

  /**
   * A simple wrapper around an inner map that delegates operations to the inner
   * set, but when iterated, looks like an empty map.
   *
   * @param <T> the key type
   * @param <V> the value type
   */
  @NotThreadSafe
  private static class BlackHoleMap<T, V> extends HashMap<T, V> {
    private Map<T, V> mInnerMap;

    /**
     * Constructs a new instance of {@link BlackHoleMap}.
     *
     * @param innerMap the inner map to use
     */
    BlackHoleMap(Map<T, V> innerMap) {
      mInnerMap = innerMap;
    }

    @Override
    public Set<T> keySet() {
      return new HashSet<>();
    }

    @Override
    public V put(T key, V value) {
      return mInnerMap.put(key, value);
    }

    @Override
    public V remove(Object key) {
      return mInnerMap.remove(key);
    }

    public Map<T, V> getInnerMap() {
      return mInnerMap;
    }
  }

  /**
   * A simple wrapper around an inner set that delegates add and remove operations to the inner
   * set, but when iterated, looks like an empty set.
   *
   * @param <T> the element type
   */
  @NotThreadSafe
  private static class BlackHoleSet<T> extends HashSet<T> {
    private Set<T> mInnerSet;

    /**
     * Constructs a new instance of {@link BlackHoleSet}.
     *
     * @param innerSet the inner set to use
     */
    BlackHoleSet(Set<T> innerSet) {
      mInnerSet = innerSet;
    }

    @Override
    public boolean add(T key) {
      return mInnerSet.add(key);
    }

    @Override
    public boolean remove(Object key) {
      return mInnerSet.remove(key);
    }

    public Set<T> getInnerSet() {
      return mInnerSet;
    }
  }

  /**
   * A convenience method to pause scheduling persist jobs.
   *
   * @param resource the local cluster resource to pause the service for
   */
  public static void pauseScheduler(LocalAlluxioClusterResource resource) {
    FileSystemMaster master = getFileSystemMaster(resource);
    Set<Long> persistRequests = Whitebox.getInternalState(master, "mPersistRequests");
    Whitebox.setInternalState(master, "mPersistRequests", new BlackHoleSet<>(persistRequests));
  }

  /**
   * A convenience method to resume scheduling persist jobs.
   *
   * @param resource the local cluster resource to resume the service for
   */
  public static void resumeScheduler(LocalAlluxioClusterResource resource) {
    FileSystemMaster master = getFileSystemMaster(resource);
    BlackHoleSet<Long> persistRequests = Whitebox.getInternalState(master, "mPersistRequests");
    Whitebox.setInternalState(master, "mPersistRequests", persistRequests.getInnerSet());
  }

  /**
   * A convenience method to pause polling persist jobs.
   *
   * @param resource the local cluster resource to pause the service for
   */
  public static void pauseChecker(LocalAlluxioClusterResource resource) {
    FileSystemMaster master = getFileSystemMaster(resource);
    Map<Long, PersistJob> persistJobs = Whitebox.getInternalState(master, "mPersistJobs");
    Whitebox.setInternalState(master, "mPersistJobs", new BlackHoleMap<>(persistJobs));
  }

  /**
   * A convenience method to resume polling persist jobs.
   *
   * @param resource the local cluster resource to resume the service for
   */
  public static void resumeChecker(LocalAlluxioClusterResource resource) {
    FileSystemMaster master = getFileSystemMaster(resource);
    BlackHoleMap<Long, PersistJob> persistJobs = Whitebox.getInternalState(master, "mPersistJobs");
    Whitebox.setInternalState(master, "mPersistJobs", persistJobs.getInnerMap());
  }

  /**
   * A convenience method to block until the persist job is scheduled for the given file ID.
   *
   * @param resource the local cluster resource to resume the service for
   * @param fileId the file ID to persist
   */
  public static void waitForJobScheduled(LocalAlluxioClusterResource resource,
      final long fileId) throws Exception {
    final FileSystemMaster master = getFileSystemMaster(resource);
    CommonUtils.waitFor(String.format("Persisted job scheduled for fileId %d", fileId),
        new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            Set<Long> requests = Whitebox.getInternalState(master, "mPersistRequests");
            return !requests.contains(fileId);
          }
        });
  }

  /**
   * A convenience method to block until the persist job is complete and processed for the given
   * file ID.
   *
   * @param resource the local cluster resource to resume the service for
   * @param fileId the file ID to persist
   */
  public static void waitForJobComplete(LocalAlluxioClusterResource resource,
      final long fileId) throws Exception {
    final FileSystemMaster master = getFileSystemMaster(resource);
    CommonUtils.waitFor(String.format("Persisted job complete for fileId %d", fileId),
        new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            Map<Long, PersistJob> jobs =
                Whitebox.getInternalState(master, "mPersistJobs");
            return !jobs.containsKey(fileId);
          }
        });
  }

  private static FileSystemMaster getFileSystemMaster(LocalAlluxioClusterResource resource) {
    return resource.get().getMaster().getInternalMaster().getMaster(FileSystemMaster.class);
  }
}
