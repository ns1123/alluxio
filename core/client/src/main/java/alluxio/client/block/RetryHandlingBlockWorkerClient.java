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

package alluxio.client.block;

import alluxio.AbstractThriftClient;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.metrics.MetricsSystem;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.thrift.ThriftIOException;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.ThriftUtils;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The client talks to a block worker server. It keeps sending keep alive message to the worker
 * server.
 *
 * Note: Every client instance is associated with a session which is usually created for each block
 * stream. So be careful when reusing this client for multiple blocks.
 */
@ThreadSafe
public final class RetryHandlingBlockWorkerClient
    extends AbstractThriftClient<BlockWorkerClientService.Client> implements BlockWorkerClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final ScheduledExecutorService HEARTBEAT_POOL = Executors.newScheduledThreadPool(
      Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_THREADS),
      ThreadFactoryUtils.build("block-worker-heartbeat-%d", true));
  private static final ExecutorService HEARTBEAT_CANCEL_POOL = Executors.newFixedThreadPool(5,
      ThreadFactoryUtils.build("block-worker-heartbeat-cancel-%d", true));
  private final BlockWorkerThriftClientPool mClientPool;
  private final BlockWorkerThriftClientPool mClientHeartbeatPool;
  // Tracks the number of active heartbeat close requests.
  private static final AtomicInteger NUM_ACTIVE_SESSIONS = new AtomicInteger(0);

  private final Long mSessionId;
  // This is the address of the data server on the worker.
  private final InetSocketAddress mWorkerDataServerAddress;
  private final WorkerNetAddress mWorkerNetAddress;
  private final InetSocketAddress mRpcAddress;

  private final ScheduledFuture<?> mHeartbeat;
  // ALLUXIO CS ADD

  /** The capability in use. This can be updated by multiple threads. */
  private volatile alluxio.security.capability.Capability mCapability = null;
  /**
   * The capability fetcher used to refresh mCapability if it becomes invalid. Normally, this
   * is only initialized once.
   */
  private volatile alluxio.client.security.CapabilityFetcher mCapabilityFetcher = null;
  // ALLUXIO CS END

  /**
   * Creates a {@link RetryHandlingBlockWorkerClient}. Set sessionId to null if no session ID is
   * required when using this client. For example, if you only call RPCs like promote, a session
   * ID is not required.
   *
   * @param clientPool the block worker client pool
   * @param clientHeartbeatPool the block worker client heartbeat pool
   * @param workerNetAddress to worker's location
   * @param sessionId the ID of the session
   * @throws IOException if it fails to register the session with the worker specified
   */
  public RetryHandlingBlockWorkerClient(
      BlockWorkerThriftClientPool clientPool,
      BlockWorkerThriftClientPool clientHeartbeatPool,
      WorkerNetAddress workerNetAddress, final Long sessionId)
      throws IOException {
    mClientPool = clientPool;
    mClientHeartbeatPool = clientHeartbeatPool;
    mRpcAddress = NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress);

    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress, "workerNetAddress");
    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mSessionId = sessionId;
    if (sessionId != null) {
      // Register the session before any RPCs for this session start.
      try {
        sessionHeartbeat();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }

      // The heartbeat is scheduled to run in a fixed rate. The heartbeat won't consume a thread
      // from the pool while it is not running.
      mHeartbeat = HEARTBEAT_POOL.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
              try {
                sessionHeartbeat();
              } catch (InterruptedException e) {
                // Do nothing.
              } catch (Exception e) {
                LOG.error("Failed to heartbeat for session " + sessionId, e);
              }
            }
          }, Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS),
          Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS), TimeUnit.MILLISECONDS);

      NUM_ACTIVE_SESSIONS.incrementAndGet();
    } else {
      mHeartbeat = null;
    }
  }

  @Override
  public BlockWorkerClientService.Client acquireClient() throws IOException {
    try {
      return mClientPool.acquire();
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void releaseClient(BlockWorkerClientService.Client client) {
    mClientPool.release(client);
  }

  @Override
  public void close() {
    if (mHeartbeat != null) {
      HEARTBEAT_CANCEL_POOL.submit(new Runnable() {
        @Override
        public void run() {
          mHeartbeat.cancel(true);
          NUM_ACTIVE_SESSIONS.decrementAndGet();
        }
      });
    }
  }

  @Override
  public WorkerNetAddress getWorkerNetAddress() {
    return mWorkerNetAddress;
  }

  @Override
  public void accessBlock(final long blockId) throws IOException {
    retryRPC(new RpcCallable<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client) throws TException {
          client.accessBlock(blockId);
          return null;
      }
    });
  }

  @Override
  public void cacheBlock(final long blockId) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client)
          throws AlluxioTException, TException {
        client.cacheBlock(getSessionId(), blockId);
        return null;
      }
    });
  }

  @Override
  public void cancelBlock(final long blockId) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client)
          throws AlluxioTException, TException {
        client.cancelBlock(getSessionId(), blockId);
        return null;
      }
    });
  }

  @Override
  public InetSocketAddress getDataServerAddress() {
    return mWorkerDataServerAddress;
  }

  @Override
  public long getSessionId() {
    Preconditions.checkNotNull(mSessionId, "SessionId is accessed when it is not supported");
    return mSessionId;
  }

  @Override
  public LockBlockResult lockBlock(final long blockId) throws IOException, AlluxioException {
    return retryRPC(
        new RpcCallableThrowsAlluxioTException<LockBlockResult, BlockWorkerClientService
            .Client>() {
          @Override
          public LockBlockResult call(BlockWorkerClientService.Client client)
              throws AlluxioTException, TException {
            // ALLUXIO CS REPLACE
            // return ThriftUtils.fromThrift(client.lockBlock(blockId, getSessionId()));
            // ALLUXIO CS WITH
            return ThriftUtils
                .fromThrift(client.lockBlock(blockId, getSessionId(), getCapability()));
            // ALLUXIO CS END
          }
        });
  }

  @Override
  public boolean promoteBlock(final long blockId) throws IOException, AlluxioException {
    return retryRPC(
        new RpcCallableThrowsAlluxioTException<Boolean, BlockWorkerClientService.Client>() {
          @Override
          public Boolean call(BlockWorkerClientService.Client client)
              throws AlluxioTException, TException {
            return client.promoteBlock(blockId);
          }
        });
  }

  @Override
  public void removeBlock(final long blockId) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client)
          throws AlluxioTException, TException {
        client.removeBlock(blockId);
        return null;
      }
    });
  }

  @Override
  public String requestBlockLocation(final long blockId, final long initialBytes,
      final int writeTier) throws IOException {
    try {
      return retryRPC(
          new RpcCallableThrowsAlluxioTException<String, BlockWorkerClientService.Client>() {
            @Override
            public String call(BlockWorkerClientService.Client client)
                throws AlluxioTException, TException {
              // ALLUXIO CS REPLACE
              // return client.requestBlockLocation(getSessionId(), blockId, initialBytes, writeTier);
              // ALLUXIO CS WITH
              return client.requestBlockLocation(getSessionId(), blockId, initialBytes, writeTier,
                  getCapability());
              // ALLUXIO CS END
            }
          });
    } catch (WorkerOutOfSpaceException e) {
      throw new IOException(ExceptionMessage.CANNOT_REQUEST_SPACE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, mRpcAddress, blockId));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean requestSpace(final long blockId, final long requestBytes) throws IOException {
    try {
      boolean success = retryRPC(
          new RpcCallableThrowsAlluxioTException<Boolean, BlockWorkerClientService.Client>() {
            @Override
            public Boolean call(BlockWorkerClientService.Client client)
                throws AlluxioTException, TException {
              return client.requestSpace(getSessionId(), blockId, requestBytes);
            }
          });
      if (!success) {
        throw new IOException(ExceptionMessage.CANNOT_REQUEST_SPACE
            .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, mRpcAddress, blockId));
      }
      return true;
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean unlockBlock(final long blockId) throws IOException {
    return retryRPC(new RpcCallable<Boolean, BlockWorkerClientService.Client>() {
      @Override
      public Boolean call(BlockWorkerClientService.Client client) throws TException {
          return client.unlockBlock(blockId, getSessionId());
      }
    });
  }

  /**
   * sessionHeartbeat is not retried because it is supposed to be called periodically.
   *
   * @throws IOException if it fails to heartbeat
   * @throws InterruptedException if heartbeat is interrupted
   */
  @Override
  public void sessionHeartbeat() throws IOException, InterruptedException {
    BlockWorkerClientService.Client client = mClientHeartbeatPool.acquire();
    try {
      client.sessionHeartbeat(getSessionId(), null);
    } catch (AlluxioTException e) {
      throw Throwables.propagate(e);
    } catch (ThriftIOException e) {
      throw new IOException(e);
    } catch (TException e) {
      client.getOutputProtocol().getTransport().close();
      throw new IOException(e);
    } finally {
      mClientHeartbeatPool.release(client);
    }
    Metrics.BLOCK_WORKER_HEATBEATS.inc();
  }

  // ALLUXIO CS ADD
  @Override
  public void updateCapability(final alluxio.security.capability.Capability capabilityIgnored)
      throws IOException, AlluxioException {
    // The parameter capabilityIgnored is not used. Instead, we use mCapability which can be updated
    // if this RPC fails because of InvalidCapabilityKey exception.
    if (mCapability == null) {
      return;
    }
    retryRPC(new RpcCallableThrowsAlluxioTException<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client)
          throws AlluxioTException, TException {
        client.updateCapability(mCapability.toThrift());
        return null;
      }
    });
  }

  @Override
  public void setCapabilityNonRPC(alluxio.security.capability.Capability capability,
      alluxio.client.security.CapabilityFetcher capabilityFetcher) {
    mCapability = capability;
    mCapabilityFetcher = capabilityFetcher;
  }

  @Override
  public void fetchAndUpdateCapability() throws IOException, AlluxioException {
    if (mCapabilityFetcher == null) {
      return;
    }
    alluxio.security.capability.Capability capability = mCapabilityFetcher.call();
    mCapability = capability;
    updateCapability(capability);
  }

  @Override
  protected <E extends Exception> void processException(BlockWorkerClientService.Client client, E e)
      throws E {
    if (!(e instanceof alluxio.exception.InvalidCapabilityException)
        || mCapabilityFetcher == null) {
      throw e;
    }
    // Refresh the capability if we see an InvalidCapabilityException exception.
    try {
      alluxio.security.capability.Capability capability = mCapabilityFetcher.call();
      if (capability == null) {
        throw e;
      }
      mCapability = capability;
      client.updateCapability(capability.toThrift());
    } catch (Exception ee) {
      throw e;
    }
  }

  /**
   * @return the capability in thrift. null is returned if the capability is not set
   */
  private alluxio.thrift.Capability getCapability() {
    alluxio.security.capability.Capability capability = mCapability;
    if (capability == null) {
      return null;
    }
    return capability.toThrift();
  }

  // ALLUXIO CS END
  /**
   * Metrics related to the {@link RetryHandlingBlockWorkerClient}.
   */
  public static final class Metrics {
    private static final Counter BLOCK_WORKER_HEATBEATS =
        MetricsSystem.clientCounter("BlockWorkerHeartbeats");

    private Metrics() {
    } // prevent instantiation
  }
}
