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

<<<<<<< HEAD
||||||| merged common ancestors
import alluxio.Constants;
import alluxio.client.WriteType;
=======
import alluxio.client.WriteType;
>>>>>>> enterprise-1.4
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.resource.CloseableResource;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio Block Store client. This is an internal client for all block level operations in Alluxio.
 * An instance of this class can be obtained via {@link AlluxioBlockStore} constructors.
 */
@ThreadSafe
public final class AlluxioBlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioBlockStore.class);

  private final FileSystemContext mContext;
  private String mLocalHostName;
  private Random mRandom;

  /**
   * Creates an Alluxio block store with default file system context and default local host name.
   *
   * @return the {@link AlluxioBlockStore} created
   */
  public static AlluxioBlockStore create() {
    return new AlluxioBlockStore(FileSystemContext.INSTANCE,
        NetworkAddressUtils.getClientHostName());
  }

  /**
   * Creates an Alluxio block store with default local hostname.
   *
   * @param context the file system context
   * @return the {@link AlluxioBlockStore} created
   */
  public static AlluxioBlockStore create(FileSystemContext context) {
    return new AlluxioBlockStore(context, NetworkAddressUtils.getClientHostName());
  }

  /**
   * Creates an Alluxio block store.
   *
   * @param context the file system context
   * @param localHostName the local hostname for the block store
   */
  public AlluxioBlockStore(FileSystemContext context, String localHostName) {
    mContext = context;
    mLocalHostName = localHostName;
    mRandom = new Random();
  }

  /**
   * Gets the block info of a block, if it exists.
   *
   * @param blockId the blockId to obtain information about
   * @return a {@link BlockInfo} containing the metadata of the block
   * @throws IOException if the block does not exist
   */
  public BlockInfo getInfo(long blockId) throws IOException {
    try (CloseableResource<BlockMasterClient> masterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      return masterClientResource.get().getBlockInfo(blockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return the info of all active block workers
   * @throws IOException when work info list cannot be obtained from master
   * @throws AlluxioException if network connection failed
   */
  public List<BlockWorkerInfo> getWorkerInfoList() throws IOException, AlluxioException {
    List<BlockWorkerInfo> infoList = new ArrayList<>();
    try (CloseableResource<BlockMasterClient> masterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      for (WorkerInfo workerInfo : masterClientResource.get().getWorkerInfoList()) {
        infoList.add(new BlockWorkerInfo(workerInfo.getAddress(), workerInfo.getCapacityBytes(),
            workerInfo.getUsedBytes()));
      }
      return infoList;
    }
  }

  /**
   * Gets a stream to read the data of a block. The stream is backed by Alluxio storage.
   *
   * @param blockId the block to read from
   * @param options the options
   * @return an {@link InputStream} which can be used to read the data in a streaming fashion
   * @throws IOException if the block does not exist
   */
  public InputStream getInStream(long blockId, InStreamOptions options)
      throws IOException {
    BlockInfo blockInfo;
    try (CloseableResource<BlockMasterClient> masterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      blockInfo = masterClientResource.get().getBlockInfo(blockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }

    if (blockInfo.getLocations().isEmpty()) {
      throw new IOException("Block " + blockId + " is not available in Alluxio");
    }
    // TODO(calvin): Get location via a policy.
    // Although blockInfo.locations are sorted by tier, we prefer reading from the local worker.
    // But when there is no local worker or there are no local blocks, we prefer the first
    // location in blockInfo.locations that is nearest to memory tier.
    // Assuming if there is no local worker, there are no local blocks in blockInfo.locations.
    // TODO(cc): Check mContext.hasLocalWorker before finding for a local block when the TODO
    // for hasLocalWorker is fixed.
    for (BlockLocation location : blockInfo.getLocations()) {
      WorkerNetAddress workerNetAddress = location.getWorkerAddress();
      if (workerNetAddress.getHost().equals(mLocalHostName)) {
        // There is a local worker and the block is local.
        try {
          return StreamFactory
              .createLocalBlockInStream(mContext, blockId, blockInfo.getLength(), workerNetAddress,
                  options);
        } catch (IOException e) {
          LOG.warn("Failed to open local stream for block " + blockId + ". " + e.getMessage());
          // Getting a local stream failed, do not try again
          break;
        }
      }
    }
    // No local worker/block, choose a random location. In the future we could change this to
    // only randomize among locations in the highest tier, or have the master randomize the order.
    List<BlockLocation> locations = blockInfo.getLocations();
    WorkerNetAddress workerNetAddress =
        locations.get(mRandom.nextInt(locations.size())).getWorkerAddress();
    return StreamFactory
        .createRemoteBlockInStream(mContext, blockId, blockInfo.getLength(), workerNetAddress,
            options);
  }

  /**
   * Gets a stream to write data to a block. The stream can only be backed by Alluxio storage.
   *
   * @param blockId the block to write
   * @param blockSize the standard block size to write, or -1 if the block already exists (and this
   *        stream is just storing the block in Alluxio again)
   * @param address the address of the worker to write the block to, fails if the worker cannot
   *        serve the request
   * @param options the output stream options
   * @return an {@link OutputStream} which can be used to write data to the block in a
   *         streaming fashion
   * @throws IOException if the block cannot be written
   */
  public OutputStream getOutStream(long blockId, long blockSize, WorkerNetAddress address,
      OutStreamOptions options) throws IOException {
    if (blockSize == -1) {
      try (CloseableResource<BlockMasterClient> blockMasterClientResource =
          mContext.acquireBlockMasterClientResource()) {
        blockSize = blockMasterClientResource.get().getBlockInfo(blockId).getLength();
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
    }
    // No specified location to write to.
    if (address == null) {
      throw new RuntimeException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    // Location is local.
    if (mLocalHostName.equals(address.getHost())) {
      return StreamFactory
          .createLocalBlockOutStream(mContext, blockId, blockSize, address, options);
    }
    // Location is specified and it is remote.
    return StreamFactory
        .createRemoteBlockOutStream(mContext, blockId, blockSize, address, options);
  }

  /**
   * Gets a stream to write data to a block based on the options. The stream can only be backed by
   * Alluxio storage.
   *
   * @param blockId the block to write
   * @param blockSize the standard block size to write, or -1 if the block already exists (and this
   *        stream is just storing the block in Alluxio again)
   * @param options the output stream option
   * @return an {@link OutputStream} which can be used to write data to the block in a
   *         streaming fashion
   * @throws IOException if the block cannot be written
   */
  public OutputStream getOutStream(long blockId, long blockSize, OutStreamOptions options)
      throws IOException {
    WorkerNetAddress address;
    FileWriteLocationPolicy locationPolicy = Preconditions.checkNotNull(options.getLocationPolicy(),
        PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED);
    // ALLUXIO CS REPLACE
    // try {
    //   address = locationPolicy.getWorkerForNextBlock(getWorkerInfoList(), blockSize);
    // } catch (AlluxioException e) {
    //   throw new IOException(e);
    // }
    // return getOutStream(blockId, blockSize, address, options);
    // ALLUXIO CS WITH
    java.util.Set<BlockWorkerInfo> blockWorkers;
    try {
      blockWorkers = com.google.common.collect.Sets.newHashSet(getWorkerInfoList());
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    // The number of initial copies depends on the write type: if ASYNC_THROUGH, it is the property
    // "alluxio.user.file.replication.durable" before data has been persisted; otherwise
    // "alluxio.user.file.replication.min"
    int initialReplicas = (options.getWriteType() == alluxio.client.WriteType.ASYNC_THROUGH
        && options.getReplicationDurable() > options.getReplicationMin())
        ? options.getReplicationDurable() : options.getReplicationMin();
    if (initialReplicas <= 1) {
      address = locationPolicy.getWorkerForNextBlock(blockWorkers, blockSize);
      return getOutStream(blockId, blockSize, address, options);
    }

    // Group different block workers by their hostnames
    java.util.Map<String, java.util.Set<BlockWorkerInfo>> blockWorkersByHost =
        new java.util.HashMap<>();
    for (BlockWorkerInfo blockWorker : blockWorkers) {
      String hostName = blockWorker.getNetAddress().getHost();
      if (blockWorkersByHost.containsKey(hostName)) {
        blockWorkersByHost.get(hostName).add(blockWorker);
      } else {
        blockWorkersByHost.put(hostName, com.google.common.collect.Sets.newHashSet(blockWorker));
      }
    }

    // Select N workers on different hosts where N is the value of initialReplicas for this block
    List<WorkerNetAddress> workerAddressList = new ArrayList<>();
    for (int i = 0; i < initialReplicas; i++) {
      address = locationPolicy.getWorkerForNextBlock(blockWorkers, blockSize);
      if (address == null) {
        break;
      }
      workerAddressList.add(address);
      blockWorkers.removeAll(blockWorkersByHost.get(address.getHost()));
    }
    if (workerAddressList.size() < initialReplicas) {
      throw new IOException(String.format(
          "Not enough workers for replications, %d workers selected but %d required",
          workerAddressList.size(), initialReplicas));
    }
    return StreamFactory
        .createReplicatedBlockOutStream(mContext, blockId, blockSize, workerAddressList, options);
    // ALLUXIO CS END
  }

  /**
   * Gets the total capacity of Alluxio's BlockStore.
   *
   * @return the capacity in bytes
   * @throws IOException when the connection to the client fails
   */
  public long getCapacityBytes() throws IOException {
    try (CloseableResource<BlockMasterClient> blockMasterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      return blockMasterClientResource.get().getCapacityBytes();
    } catch (ConnectionFailedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Gets the used bytes of Alluxio's BlockStore.
   *
   * @return the used bytes of Alluxio's BlockStore
   * @throws IOException when the connection to the client fails
   */
  public long getUsedBytes() throws IOException {
    try (CloseableResource<BlockMasterClient> blockMasterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      return blockMasterClientResource.get().getUsedBytes();
    } catch (ConnectionFailedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Attempts to promote a block in Alluxio space. If the block is not present, this method will
   * return without an error. If the block is present in multiple workers, only one worker will
   * receive the promotion request.
   *
   * @param blockId the id of the block to promote
   // ALLUXIO CS ADD
   * @param capabilityFetcher the capability fetcher
   // ALLUXIO CS END
   * @throws IOException if the block does not exist
   */
  // ALLUXIO CS REPLACE
  // public void promote(long blockId) throws IOException {
  // ALLUXIO CS WITH
  public void promote(long blockId, alluxio.client.security.CapabilityFetcher capabilityFetcher)
      throws IOException {
    // ALLUXIO CS END
    BlockInfo info;
    try (CloseableResource<BlockMasterClient> blockMasterClientResource =
        mContext.acquireBlockMasterClientResource()) {
      info = blockMasterClientResource.get().getBlockInfo(blockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    if (info.getLocations().isEmpty()) {
      // Nothing to promote
      return;
    }
    // Get the first worker address for now, as this will likely be the location being read from
    // TODO(calvin): Get this location via a policy (possibly location is a parameter to promote)
    BlockWorkerClient blockWorkerClient = mContext.createBlockWorkerClient(
        info.getLocations().get(0).getWorkerAddress(), null  /* no session */);
    // ALLUXIO CS ADD
    blockWorkerClient.setCapabilityNonRPC(capabilityFetcher);
    // ALLUXIO CS END
    try {
      blockWorkerClient.promoteBlock(blockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      blockWorkerClient.close();
    }
  }

  /**
   * Sets the local host name. This is only used in the test.
   *
   * @param localHostName the local host name
   */
  public void setLocalHostName(String localHostName) {
    mLocalHostName = localHostName;
  }
}
