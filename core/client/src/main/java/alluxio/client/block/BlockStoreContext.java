/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.resource.CloseableResource;
import alluxio.util.IdUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.ClientMetrics;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
<<<<<<< HEAD:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
import java.util.ArrayList;
import java.util.HashMap;
||||||| merged common ancestors
=======
import java.util.ArrayList;
>>>>>>> OPENSOURCE/master:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
import java.util.List;
<<<<<<< HEAD:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
import java.util.Map;
||||||| merged common ancestors
=======
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
>>>>>>> OPENSOURCE/master:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java

import javax.annotation.concurrent.ThreadSafe;

/**
 * A shared context in each client JVM for common block master client functionality such as a pool
 * of master clients and a pool of local worker clients. Any remote clients will be created and
 * destroyed on a per use basis.
 * <p/>
 * NOTE: The context maintains a pool of block master clients and a pool of block worker clients
 * that are already thread-safe. Synchronizing {@link BlockStoreContext} methods could lead to
 * deadlock: thread A attempts to acquire a client when there are no clients left in the pool and
 * blocks holding a lock on the {@link BlockStoreContext}, when thread B attempts to release a
 * client it owns, it is unable to do so, because thread A holds the lock on
 * {@link BlockStoreContext}.
 */
@ThreadSafe
public enum BlockStoreContext {
  INSTANCE;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockMasterClientPool mBlockMasterClientPool;
<<<<<<< HEAD:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
  /** A map from the worker's address to its client pool. */
  private Map<WorkerNetAddress, BlockWorkerClientPool> mLocalBlockWorkerClientPoolMap =
      new HashMap<>();
||||||| merged common ancestors
  private BlockWorkerClientPool mLocalBlockWorkerClientPool;
=======

  /**
   * A map from the worker's address to its client pool. Guarded by
   * {@link #initializeLocalBlockWorkerClientPool()} for client acquisition. There is no guard for
   * releasing client, and client can be released anytime.
   */
  private final Map<WorkerNetAddress, BlockWorkerClientPool> mLocalBlockWorkerClientPoolMap =
      new ConcurrentHashMap<>();
>>>>>>> OPENSOURCE/master:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java

  private boolean mLocalBlockWorkerClientPoolInitialized = false;

  /**
   * Creates a new block store context.
   */
  BlockStoreContext() {
    reset();
  }

  /**
   * Initializes {@link #mLocalBlockWorkerClientPoolMap}. This method is supposed be called in a
   * lazy manner.
   */
  private synchronized void initializeLocalBlockWorkerClientPool() {
    if (!mLocalBlockWorkerClientPoolInitialized) {
      for (WorkerNetAddress localWorkerAddress : getWorkerAddresses(
          NetworkAddressUtils.getLocalHostName(ClientContext.getConf()))) {
        mLocalBlockWorkerClientPoolMap.put(localWorkerAddress,
            new BlockWorkerClientPool(localWorkerAddress));
      }
      mLocalBlockWorkerClientPoolInitialized = true;
    }
  }

  /**
<<<<<<< HEAD:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
   * Gets the worker addresses with the given hostname by querying the master.
||||||| merged common ancestors
   * Gets the worker address based on its hostname by querying the master.
=======
   * Gets the worker addresses with the given hostname by querying the master. Returns all the
   * addresses, if the hostname is an empty string.
>>>>>>> OPENSOURCE/master:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
   *
   * @param hostname hostname of the worker to query, empty string denotes any worker
<<<<<<< HEAD:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
   * @return {@link List} of {@link WorkerNetAddress} of hostname
||||||| merged common ancestors
   * @return {@link WorkerNetAddress} of hostname, or null if no worker found
=======
   * @return List of {@link WorkerNetAddress} of hostname
>>>>>>> OPENSOURCE/master:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
   */
<<<<<<< HEAD:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
  private List<WorkerNetAddress> getWorkerAddresses(String hostname) {
    BlockMasterClient masterClient = acquireMasterClient();
    List<WorkerNetAddress> addresses = new ArrayList<>();
    try {
      List<WorkerInfo> workers = masterClient.getWorkerInfoList();
||||||| merged common ancestors
  private WorkerNetAddress getWorkerAddress(String hostname) {
    BlockMasterClient masterClient = acquireMasterClient();
    try {
      List<WorkerInfo> workers = masterClient.getWorkerInfoList();
      if (hostname.isEmpty() && !workers.isEmpty()) {
        // TODO(calvin): Do this in a more defined way.
        return workers.get(0).getAddress();
      }
=======
  private List<WorkerNetAddress> getWorkerAddresses(String hostname) {
    List<WorkerNetAddress> addresses = new ArrayList<>();
    try (CloseableResource<BlockMasterClient> masterClient = acquireMasterClientResource()) {
      List<WorkerInfo> workers = masterClient.get().getWorkerInfoList();
>>>>>>> OPENSOURCE/master:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
      for (WorkerInfo worker : workers) {
        if (hostname.isEmpty() || worker.getAddress().getHost().equals(hostname)) {
          addresses.add(worker.getAddress());
        }
      }
    } catch (Exception e) {
      Throwables.propagate(e);
<<<<<<< HEAD:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
    } finally {
      releaseMasterClient(masterClient);
    }
    return addresses;
  }

  /**
   * Acquires a block master client from the block master client pool.
   *
   * @return the acquired block master client
   */
  public BlockMasterClient acquireMasterClient() {
    return mBlockMasterClientPool.acquire();
  }

  /**
   * Releases a block master client into the block master client pool.
   *
   * @param masterClient a block master client to release
   */
  public void releaseMasterClient(BlockMasterClient masterClient) {
    mBlockMasterClientPool.release(masterClient);
  }

  /**
   * Obtains a worker client to a worker in the system. A local client is preferred to be returned
   * but not guaranteed. The caller should use {@link BlockWorkerClient#isLocal()} to verify if the
   * client is local before assuming so.
   *
   * @return a {@link BlockWorkerClient} to a worker in the Alluxio system
   */
  public BlockWorkerClient acquireWorkerClient() {
    BlockWorkerClient client = acquireLocalWorkerClient();
    if (client == null) {
      // Get a worker client for any worker in the system.
      List<WorkerNetAddress> workerAddresses = getWorkerAddresses("");
      if (workerAddresses.isEmpty()) {
        return acquireRemoteWorkerClient(null);
||||||| merged common ancestors
    } finally {
      releaseMasterClient(masterClient);
    }
    return null;
  }

  /**
   * Acquires a block master client from the block master client pool.
   *
   * @return the acquired block master client
   */
  public BlockMasterClient acquireMasterClient() {
    return mBlockMasterClientPool.acquire();
  }

  /**
   * Releases a block master client into the block master client pool.
   *
   * @param masterClient a block master client to release
   */
  public void releaseMasterClient(BlockMasterClient masterClient) {
    mBlockMasterClientPool.release(masterClient);
  }

  /**
   * Obtains a worker client to a worker in the system. A local client is preferred to be returned
   * but not guaranteed. The caller should use {@link BlockWorkerClient#isLocal()} to verify if the
   * client is local before assuming so.
   *
   * @return a {@link BlockWorkerClient} to a worker in the Alluxio system
   */
  public BlockWorkerClient acquireWorkerClient() {
    BlockWorkerClient client = acquireLocalWorkerClient();
    if (client == null) {
      // Get a worker client for any worker in the system.
      return acquireRemoteWorkerClient("");
    }
    return client;
  }

  /**
   * Obtains a worker client to the worker with the given hostname in the system.
   *
   * @param hostname the hostname of the worker to get a client to, empty String indicates all
   *        workers are eligible
   * @return a {@link BlockWorkerClient} connected to the worker with the given hostname
   * @throws IOException if no Alluxio worker is available for the given hostname
   */
  public BlockWorkerClient acquireWorkerClient(String hostname) throws IOException {
    BlockWorkerClient client;
    if (hostname.equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()))) {
      client = acquireLocalWorkerClient();
      if (client == null) {
        throw new IOException(ExceptionMessage.NO_WORKER_AVAILABLE_ON_HOST.getMessage(hostname));
=======
    }
    return addresses;
  }

  /**
   * Acquires a block master client resource from the block master client pool. The resource is
   * {@code Closeable}.
   *
   * @return the acquired block master client resource
   */
  public CloseableResource<BlockMasterClient> acquireMasterClientResource() {
    return new CloseableResource<BlockMasterClient>(mBlockMasterClientPool.acquire()) {
      @Override
      public void close() {
        mBlockMasterClientPool.release(get());
>>>>>>> OPENSOURCE/master:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
      }
<<<<<<< HEAD:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
      return acquireRemoteWorkerClient(workerAddresses.get(0));
    }
    return client;
||||||| merged common ancestors
    } else {
      client = acquireRemoteWorkerClient(hostname);
    }
    return client;
=======
    };
>>>>>>> OPENSOURCE/master:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
  }

  /**
   * Obtains a client for a worker with the given address.
   *
   * @param address the address of the worker to get a client to
   * @return a {@link BlockWorkerClient} connected to the worker with the given hostname
   * @throws IOException if no Alluxio worker is available for the given hostname
   */
  public BlockWorkerClient acquireWorkerClient(WorkerNetAddress address) throws IOException {
    BlockWorkerClient client;
    if (address == null) {
      throw new RuntimeException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    if (address.getHost().equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()))) {
      client = acquireLocalWorkerClient(address);
      if (client == null) {
        throw new IOException(
            ExceptionMessage.NO_WORKER_AVAILABLE_ON_ADDRESS.getMessage(address));
      }
    } else {
      client = acquireRemoteWorkerClient(address);
    }
    return client;
  }

  /**
   * Obtains a worker client on the local worker in the system. For testing only.
   *
   * @return a {@link BlockWorkerClient} to a worker in the Alluxio system or null if failed
   */
  public BlockWorkerClient acquireLocalWorkerClient() {
    initializeLocalBlockWorkerClientPool();
    if (mLocalBlockWorkerClientPoolMap.isEmpty()) {
      return null;
    }
    // return any local worker
    return mLocalBlockWorkerClientPoolMap.values().iterator().next().acquire();
  }

  /**
   * Obtains a worker client for the given local worker address.
   *
   * @param address worker address
   *
   * @return a {@link BlockWorkerClient} to the given worker address or null if no such worker can
   *         be found
   */
  public BlockWorkerClient acquireLocalWorkerClient(WorkerNetAddress address) {
    initializeLocalBlockWorkerClientPool();
    if (!mLocalBlockWorkerClientPoolMap.containsKey(address)) {
      return null;
    }
    return mLocalBlockWorkerClientPoolMap.get(address).acquire();
  }

  /**
   * Obtains a client for a remote based on the given network address. Illegal argument exception is
   * thrown if the hostname is the local hostname. Runtime exception is thrown if the client cannot
   * be created with a connection to the hostname.
   *
   * @param address the address of the worker
   * @return a worker client with a connection to the specified hostname
   */
  private BlockWorkerClient acquireRemoteWorkerClient(WorkerNetAddress address) {
    // If we couldn't find a worker, crash.
    if (address == null) {
      // TODO(calvin): Better exception usage.
      throw new RuntimeException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    Preconditions.checkArgument(
        !address.getHost().equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf())),
        PreconditionMessage.REMOTE_CLIENT_BUT_LOCAL_HOSTNAME);
    long clientId = IdUtils.getRandomNonNegativeLong();
    return new BlockWorkerClient(address, ClientContext.getExecutorService(),
        ClientContext.getConf(), clientId, false, new ClientMetrics());
  }

  /**
   * Releases the {@link BlockWorkerClient} back to the client pool, or destroys it if it was a
   * remote client.
   *
   * @param blockWorkerClient the worker client to release, the client should not be accessed after
   *        this method is called
   */
  public void releaseWorkerClient(BlockWorkerClient blockWorkerClient) {
    // If the client is local and the pool exists, release the client to the pool, otherwise just
    // close the client.
    if (blockWorkerClient.isLocal()) {
      // Return local worker client to its resource pool.
<<<<<<< HEAD:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
      WorkerNetAddress address = blockWorkerClient.getWorkerNetAddress();
      Preconditions.checkState(mLocalBlockWorkerClientPoolMap.containsKey(address));
      mLocalBlockWorkerClientPoolMap.get(address).release(blockWorkerClient);
||||||| merged common ancestors
      Preconditions.checkState(mLocalBlockWorkerClientPool != null);
      mLocalBlockWorkerClientPool.release(blockWorkerClient);
=======
      WorkerNetAddress address = blockWorkerClient.getWorkerNetAddress();
      if (!mLocalBlockWorkerClientPoolMap.containsKey(address)) {
        LOG.error("The client to worker at {} to release is no longer registered in the context.",
            address);
        blockWorkerClient.close();
      } else {
        mLocalBlockWorkerClientPoolMap.get(address).release(blockWorkerClient);
      }
>>>>>>> OPENSOURCE/master:core/client/src/main/java/alluxio/client/block/BlockStoreContext.java
    } else {
      // Destroy remote worker client.
      blockWorkerClient.close();
    }
  }

  /**
   * Re-initializes the {@link BlockStoreContext}. This method should only be used in
   * {@link ClientContext}.
   */
  @SuppressFBWarnings
  public void reset() {
    if (mBlockMasterClientPool != null) {
      mBlockMasterClientPool.close();
    }
    for (BlockWorkerClientPool pool : mLocalBlockWorkerClientPoolMap.values()) {
      pool.close();
    }
    mLocalBlockWorkerClientPoolMap.clear();
    mBlockMasterClientPool = new BlockMasterClientPool(ClientContext.getMasterAddress());
    mLocalBlockWorkerClientPoolInitialized = false;
  }
}
