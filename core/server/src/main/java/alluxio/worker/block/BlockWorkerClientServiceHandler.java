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

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.exception.WorkerOutOfSpaceException;
// ALLUXIO CS ADD
import alluxio.security.authorization.Mode;
// ALLUXIO CS END
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.thrift.LockBlockResult;
import alluxio.thrift.TWriteTier;
import alluxio.thrift.ThriftIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for block worker RPCs invoked by an Alluxio client.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class BlockWorkerClientServiceHandler implements BlockWorkerClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block Worker handle that carries out most of the operations. */
  private final BlockWorker mWorker;
  /** Association between storage tier aliases and ordinals ond this worker. */
  private final StorageTierAssoc mStorageTierAssoc;
  // ALLUXIO CS ADD
  private final boolean mCapabilityEnabled = alluxio.Configuration
      .getBoolean(alluxio.PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_ENABLED);
  // ALLUXIO CS END

  /**
   * Creates a new instance of {@link BlockWorkerClientServiceHandler}.
   *
   * @param worker block worker handler
   */
  public BlockWorkerClientServiceHandler(BlockWorker worker) {
    mWorker = worker;
    mStorageTierAssoc = new WorkerStorageTierAssoc();
  }

  @Override
  public long getServiceVersion() {
    return Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION;
  }

  /**
   * This should be called whenever a client does a direct read in order to update the worker's
   * components that may care about the access times of the blocks (for example, Evictor, UI).
   *
   * @param blockId the id of the block to access
   * @throws AlluxioTException if an Alluxio error occurs
   */
  @Override
  public void accessBlock(final long blockId) throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        // ALLUXIO CS ADD
        checkAccessMode(blockId, Mode.Bits.READ);
        // ALLUXIO CS END
        mWorker.accessBlock(Sessions.ACCESS_BLOCK_SESSION_ID, blockId);
        return null;
      }
    });
  }

  /**
   * Used to cache a block into Alluxio space, worker will move the temporary block file from
   * session folder to data folder, and update the space usage information related. then update the
   * block information to master.
   *
   * @param sessionId the id of the client requesting the commit
   * @param blockId the id of the block to commit
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public void cacheBlock(final long sessionId, final long blockId)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        // ALLUXIO CS ADD
        checkAccessMode(blockId, Mode.Bits.READ_WRITE);
        // ALLUXIO CS END
        mWorker.commitBlock(sessionId, blockId);
        return null;
      }
    });
  }

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   *
   * @param sessionId the id of the client requesting the abort
   * @param blockId the id of the block to be aborted
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public void cancelBlock(final long sessionId, final long blockId)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        // ALLUXIO CS ADD
        checkAccessMode(blockId, Mode.Bits.READ_WRITE);
        // ALLUXIO CS END
        mWorker.abortBlock(sessionId, blockId);
        return null;
      }
    });
  }

  /**
   * Locks the file in Alluxio's space while the session is reading it.
   *
   * @param blockId the id of the block to be locked
   * @param sessionId the id of the session
   * @return the path of the block file locked
   * @throws AlluxioTException if an Alluxio error occurs
   */
  @Override
  // ALLUXIO CS REPLACE
  // public LockBlockResult lockBlock(final long blockId, final long sessionId)
  //     throws AlluxioTException {
  // ALLUXIO CS WITH
  public LockBlockResult lockBlock(final long blockId, final long sessionId,
      final alluxio.thrift.Capability capability) throws AlluxioTException {
    // ALLUXIO CS END
    return RpcUtils.call(new RpcCallable<LockBlockResult>() {
      @Override
      public LockBlockResult call() throws AlluxioException {
        // ALLUXIO CS ADD
        mWorker.getCapabilityCache().addCapability(capability);
        checkAccessMode(blockId, Mode.Bits.READ);
        // ALLUXIO CS END
        long lockId = mWorker.lockBlock(sessionId, blockId);
        return new LockBlockResult(lockId, mWorker.readBlock(sessionId, blockId, lockId));
      }
    });
  }

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Alluxio's space.
   * otherwise.
   *
   * @param blockId the id of the block to move to the top layer
   * @return true if the block is successfully promoted, otherwise false
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  // TODO(calvin): This may be better as void.
  @Override
  public boolean promoteBlock(final long blockId) throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(new RpcCallableThrowsIOException<Boolean>() {
      @Override
      public Boolean call() throws AlluxioException, IOException {
        // ALLUXIO CS ADD
        checkAccessMode(blockId, Mode.Bits.READ_WRITE);
        // ALLUXIO CS END
        // TODO(calvin): Make the top level configurable.
        mWorker.moveBlock(Sessions.MIGRATE_DATA_SESSION_ID, blockId, mStorageTierAssoc.getAlias(0));
        return true;
      }
    });
  }

  /**
   * Used to remove a block in Alluxio storage. Worker will delete the block file and
   * reclaim space allocated to the block.
   *
   * @param blockId the id of the block to be removed
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public void removeBlock(final long blockId)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mWorker.removeBlock(Sessions.MIGRATE_DATA_SESSION_ID, blockId);
        return null;
      }
    });
  }

  /**
   * Convert a relative policy such as HIGHEST or LOWEST into a tier id.
   *
   * @param tWriteTier the relative policy used for block allocation
   * @return the tier level to use for allocation
   */
  private int getLevelFromPolicy(final TWriteTier tWriteTier) {
    if (tWriteTier == null) {
      return 0;
    }

    switch (tWriteTier) {
      case Highest:
        return 0;
      case SecondHighest:
        if (mStorageTierAssoc.size() == 1) {
          /* With only one tier, use it */
          return 0;
        }
        return 1;
      case Lowest:
        return mStorageTierAssoc.size() - 1;
      default:
        return 0;
    }
  }

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy. If there is
   * not enough space on Alluxio storage {@link alluxio.exception.WorkerOutOfSpaceException} will be
   * thrown, if the file is already being written by the session,
   * {@link alluxio.exception.FileAlreadyExistsException} will be thrown.
   *
   * @param sessionId the id of the client requesting the create
   * @param blockId the id of the new block to create
   * @param initialBytes the initial number of bytes to allocate for this block
   * @param writeTier policy used to choose tier for this block
   * @return the temporary file path of the block file
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public String requestBlockLocation(final long sessionId, final long blockId,
      // ALLUXIO CS REPLACE
      // final long initialBytes, final TWriteTier writeTier)
      // throws AlluxioTException, ThriftIOException {
      // ALLUXIO CS WITH
      final long initialBytes, final TWriteTier writeTier,
      final alluxio.thrift.Capability capability) throws AlluxioTException, ThriftIOException {
    // ALLUXIO CS END
    return RpcUtils.call(new RpcCallableThrowsIOException<String>() {
      @Override
      public String call() throws AlluxioException, IOException {
        // ALLUXIO CS ADD
        mWorker.getCapabilityCache().addCapability(capability);
        checkAccessMode(blockId, Mode.Bits.READ_WRITE);
        // ALLUXIO CS END
        // NOTE: right now, we ask allocator to allocate new blocks in top tier
        int level = getLevelFromPolicy(writeTier);
        return mWorker.createBlock(sessionId, blockId, mStorageTierAssoc.getAlias(level),
            initialBytes);
      }
    });
  }

  /**
   * Requests space for a block.
   *
   * @param sessionId the id of the client requesting space
   * @param blockId the id of the block to add the space to, this must be a temporary block
   * @param requestBytes the amount of bytes to add to the block
   * @return true if the worker successfully allocates space for the block on block’s location,
   *         false if there is not enough space
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public boolean requestSpace(final long sessionId, final long blockId, final long requestBytes)
      throws AlluxioTException, ThriftIOException {
    return RpcUtils.call(new RpcCallable<Boolean>() {
      @Override
      public Boolean call() throws AlluxioException {
        try {
          // ALLUXIO CS ADD
          checkAccessMode(blockId, Mode.Bits.READ_WRITE);
          // ALLUXIO CS END
          mWorker.requestSpace(sessionId, blockId, requestBytes);
          return true;
        } catch (WorkerOutOfSpaceException e) {
          LOG.warn("Worker is out of space, failed to serve request for {} bytes for block {}",
              requestBytes, blockId);
          return false;
        } catch (IOException e) {
          LOG.error("Failed to serve request for {} bytes for block: {}", requestBytes, blockId, e);
          // We must wrap IOException in an AlluxioException here for backwards compatibility with
          // previous versions of our API.
          throw new UnexpectedAlluxioException(e);
        } catch (BlockDoesNotExistException e) {
          LOG.error("Failed to serve request for {} bytes for block: {}", requestBytes, blockId, e);
          throw e;
        }
      }
    });
  }

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file.
   *
   * @param blockId the id of the block to unlock
   * @param sessionId the id of the client requesting the unlock
   * @return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block
   * @throws AlluxioTException if an Alluxio error occurs
   */
  // TODO(andrew): This should return void
  @Override
  public boolean unlockBlock(final long blockId, final long sessionId) throws AlluxioTException {
    return RpcUtils.call(new RpcCallable<Boolean>() {
      @Override
      public Boolean call() throws AlluxioException {
        // ALLUXIO CS ADD
        checkAccessMode(blockId, Mode.Bits.READ);
        // ALLUXIO CS END
        mWorker.unlockBlock(sessionId, blockId);
        return true;
      }
    });
  }

  /**
   * Local session send heartbeat to local worker to keep its temporary folder.
   *
   * @param sessionId the id of the client heartbeating
   * @param metrics deprecated
   */
  @Override
  public void sessionHeartbeat(final long sessionId, final List<Long> metrics)
      throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mWorker.sessionHeartbeat(sessionId);
        return null;
      }
    });
  }
  // ALLUXIO CS ADD

  @Override
  public void updateCapability(final alluxio.thrift.Capability capability)
      throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mWorker.getCapabilityCache().addCapability(capability);
        return null;
      }
    });
  }

  /**
   * Checks whether the current user has the permission to access a block in a given mode.
   *
   * @param blockId the block ID
   * @param accessMode the requested access mode
   * @throws alluxio.exception.InvalidCapabilityException if the capability is expired or invalid
   * @throws alluxio.exception.AccessControlException if the user does not have access
   */
  private void checkAccessMode(long blockId, alluxio.security.authorization.Mode.Bits accessMode)
      throws alluxio.exception.InvalidCapabilityException,
      alluxio.exception.AccessControlException {
    if (!mCapabilityEnabled) {
      return;
    }
    String user = alluxio.security.authentication.AuthenticatedClientUser.getClientUser();
    mWorker.getCapabilityCache()
        .checkAccess(user, alluxio.util.IdUtils.fileIdFromBlockId(blockId), accessMode);
  }
  // ALLUXIO CS END
}
