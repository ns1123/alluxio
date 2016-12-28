package alluxio;

import alluxio.master.file.FileSystemMaster;

import org.apache.commons.lang3.tuple.Triple;
import org.powermock.reflect.Whitebox;

import java.util.HashSet;
import java.util.Iterator;
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
  private static class BlackHole<T> extends HashSet<T> {
    private Set<T> mInnerSet;

    /**
     * Constructs a new instance of {@link BlackHole}.
     *
     * @param innerSet the inner set to use
     */
    BlackHole(Set<T> innerSet) {
      mInnerSet = innerSet;
    }

    @Override
    public Iterator<T> iterator() {
      return new EmptyIterator<>();
    }

    @Override
    public boolean add(T e) {
      return mInnerSet.add(e);
    }

    @Override
    public boolean remove(Object e) {
      return mInnerSet.remove(e);
    }

    public Set<T> getInnerSet() {
      return mInnerSet;
    }
  }

  /**
   * A convenience method to pause async persist service.
   *
   * @param resource the local cluster resource to pause the service for
   */
  public static void pauseAsyncPersist(LocalAlluxioClusterResource resource) {
    FileSystemMaster master = getFileSystemMaster(resource);
    Set<Long> filesToPersist = Whitebox.getInternalState(master, "mFilesToPersist");
    Set<Triple<Long, Long, String>> persistJobs = Whitebox.getInternalState(master, "mPersistJobs");
    Whitebox.setInternalState(master, "mFilesToPersist", new BlackHole<>(filesToPersist));
    Whitebox.setInternalState(master, "mPersistJobs", new BlackHole<>(persistJobs));
  }

  /**
   * A convenience method to resume async persist service.
   *
   * @param resource the local cluster resource to resume the service for
   */
  public static void resumeAsyncPersist(LocalAlluxioClusterResource resource) {
    FileSystemMaster master = getFileSystemMaster(resource);
    BlackHole<Long> filesToPersist = Whitebox.getInternalState(master, "mFilesToPersist");
    BlackHole<Triple<Long, Long, String>> persistJobs =
        Whitebox.getInternalState(master, "mPersistJobs");
    Whitebox.setInternalState(master, "mFilesToPersist", filesToPersist.getInnerSet());
    Whitebox.setInternalState(master, "mPersistJobs", persistJobs.getInnerSet());
  }

  private static FileSystemMaster getFileSystemMaster(LocalAlluxioClusterResource resource) {
    return resource.get().getMaster().getInternalMaster().getFileSystemMaster();
  }
}
