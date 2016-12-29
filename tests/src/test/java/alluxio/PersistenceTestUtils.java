package alluxio;

import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.PersistJob;
import alluxio.master.file.PersistRequest;

import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Util methods for persistence integration tests.
 */
public class PersistenceTestUtils {

  /**
   * Iterator that makes any set look like an empty set.
   *
   * @param <T> the element type
   */
  @ThreadSafe
  private static class EmptyIterator<T> implements Iterator<T> {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public T next() {
      return null;
    }

    @Override
    public void remove() {}
  }

  /**
   * A simple wrapper around an inner set that delegates add and remove operations to the inner
   * set, but when iterated, looks like an empty set.
   *
   * @param <T> the element type
   */
  @NotThreadSafe
  private static class BlackHole<T,V> extends HashMap<T,V> {
    private Map<T,V> mInnerMap;

    /**
     * Constructs a new instance of {@link BlackHole}.
     *
     * @param innerMap the inner map to use
     */
    BlackHole(Map<T,V> innerMap) {
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

    public Map<T,V> getInnerMap() {
      return mInnerMap;
    }
  }

  /**
   * A convenience method to pause async persist service.
   *
   * @param resource the local cluster resource to pause the service for
   */
  public static void pauseAsyncPersist(LocalAlluxioClusterResource resource) {
    FileSystemMaster master = getFileSystemMaster(resource);
    Map<Long, PersistRequest> persistRequests =
        Whitebox.getInternalState(master, "mPersistRequests");
    Map<Long, PersistJob> persistJobs = Whitebox.getInternalState(master, "mPersistJobs");
    Whitebox.setInternalState(master, "mPersistRequests", new BlackHole<>(persistRequests));
    Whitebox.setInternalState(master, "mPersistJobs", new BlackHole<>(persistJobs));
  }

  /**
   * A convenience method to resume async persist service.
   *
   * @param resource the local cluster resource to resume the service for
   */
  public static void resumeAsyncPersist(LocalAlluxioClusterResource resource) {
    FileSystemMaster master = getFileSystemMaster(resource);
    BlackHole<Long, PersistRequest> persistRequests =
        Whitebox.getInternalState(master, "mPersistRequests");
    BlackHole<Long, PersistJob> persistJobs = Whitebox.getInternalState(master, "mPersistJobs");
    Whitebox.setInternalState(master, "mPersistRequests", persistRequests.getInnerMap());
    Whitebox.setInternalState(master, "mPersistJobs", persistJobs.getInnerMap());
  }

  private static FileSystemMaster getFileSystemMaster(LocalAlluxioClusterResource resource) {
    return resource.get().getMaster().getInternalMaster().getFileSystemMaster();
  }
}
