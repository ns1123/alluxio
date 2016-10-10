package alluxio.master.file.replication;

import alluxio.exception.AlluxioException;

/**
 * The default implementation that utilizes job service.
 */
public final class DefaultEvictHandler implements EvictHandler {

  /**
   * Constructs an instance of {@link DefaultEvictHandler}.
   */
  public DefaultEvictHandler() {
  }

  @Override
  public void scheduleEvict(long blockId, int numReplicas) throws AlluxioException {
    // TODO(binfan): implement this using REST api
  }
}
