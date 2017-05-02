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

package alluxio.master.block;

import alluxio.StorageTierAssoc;
import alluxio.exception.BlockInfoException;
import alluxio.exception.NoWorkerException;
import alluxio.master.Master;
import alluxio.thrift.Command;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface of the block master that manages the metadata for all the blocks and block workers in
 * Alluxio.
 */
<<<<<<< HEAD
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class BlockMaster extends AbstractMaster implements ContainerIdGenerable {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMaster.class);

  /**
   * The number of container ids to 'reserve' before having to journal container id state. This
   * allows the master to return container ids within the reservation, without having to write to
   * the journal.
   */
  private static final long CONTAINER_ID_RESERVATION_SIZE = 1000;

  // Worker metadata management.
  private static final IndexDefinition<MasterWorkerInfo> ID_INDEX =
      new IndexDefinition<MasterWorkerInfo>(true) {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<MasterWorkerInfo> ADDRESS_INDEX =
      new IndexDefinition<MasterWorkerInfo>(true) {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getWorkerAddress();
        }
      };

  /**
   * Concurrency and locking in the BlockMaster
   *
   * The block master uses concurrent data structures to allow non-conflicting concurrent access.
   * This means each piece of metadata should be locked individually. There are two types of
   * metadata in the {@link BlockMaster}; {@link MasterBlockInfo} and {@link MasterWorkerInfo}.
   * Individual objects must be locked before modifying the object, or reading a modifiable field
   * of an object. This will protect the internal integrity of the metadata object.
   *
   * Lock ordering must be preserved in order to prevent deadlock. If both a worker and block
   * metadata must be locked at the same time, the worker metadata ({@link MasterWorkerInfo})
   * must be locked before the block metadata ({@link MasterBlockInfo}).
   *
   * It should not be the case that multiple worker metadata must be locked at the same time, or
   * multiple block metadata must be locked at the same time. Operations involving different
   * workers or different blocks should be able to be performed independently.
   */

  // Block metadata management.
  /** Blocks on all workers, including active and lost blocks. This state must be journaled. */
  private final ConcurrentHashMapV8<Long, MasterBlockInfo> mBlocks =
      new ConcurrentHashMapV8<>(8192, 0.90f, 64);
  /** Keeps track of blocks which are no longer in Alluxio storage. */
  private final ConcurrentHashSet<Long> mLostBlocks = new ConcurrentHashSet<>(64, 0.90f, 64);

  /** This state must be journaled. */
  @GuardedBy("itself")
  private final BlockContainerIdGenerator mBlockContainerIdGenerator =
      new BlockContainerIdGenerator();

  /**
   * Mapping between all possible storage level aliases and their ordinal position. This mapping
   * forms a total ordering on all storage level aliases in the system, and must be consistent
   * across masters.
   */
  private StorageTierAssoc mGlobalStorageTierAssoc;

  /** Keeps track of workers which are in communication with the master. */
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);
  /** Keeps track of workers which are no longer in communication with the master. */
  private final IndexedSet<MasterWorkerInfo> mLostWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);

  /**
   * The service that detects lost worker nodes, and tries to restart the failed workers.
   * We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLostWorkerDetectionService;

  /** The value of the 'next container id' last journaled. */
  @GuardedBy("mBlockContainerIdGenerator")
  private long mJournaledNextContainerId = 0;

  // ALLUXIO CS ADD
  private volatile long mMaxWorkers = 0;

  /**
   * Whether the capability feature used to authorize the Alluxio data path is enabled.
   */
  private final boolean mCapabilityEnabled =
      Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_ENABLED);

  private final long mCapabilityLifetimeMs =
      Configuration.getLong(PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_LIFETIME_MS);

  private final long mCapabilityKeyLifetimeMs =
      Configuration.getLong(PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_KEY_LIFETIME_MS);

  private alluxio.master.security.capability.CapabilityKeyManager mCapabilityKeyManager = null;
  // ALLUXIO CS END
  /**
   * Creates a new instance of {@link BlockMaster}.
   *
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  BlockMaster(JournalFactory journalFactory) {
    this(journalFactory, new SystemClock(), ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.BLOCK_MASTER_NAME, 2));
  }

  /**
   * Creates a new instance of {@link BlockMaster}.
   *
   * @param journalFactory the factory for the journal to use for tracking master operations
   * @param clock the clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  BlockMaster(JournalFactory journalFactory, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    super(journalFactory.create(Constants.BLOCK_MASTER_NAME), clock, executorServiceFactory);
    Metrics.registerGauges(this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME,
        new BlockMasterClientService.Processor<>(new BlockMasterClientServiceHandler(this)));
    services.put(Constants.BLOCK_MASTER_WORKER_SERVICE_NAME,
        new BlockMasterWorkerService.Processor<>(new BlockMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.BLOCK_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    if (entry.getSequenceNumber() == 0) {
      // This is the first journal entry, clear the master state.
      mBlocks.clear();
    }
    // TODO(gene): A better way to process entries besides a huge switch?
    if (entry.hasBlockContainerIdGenerator()) {
      mJournaledNextContainerId = (entry.getBlockContainerIdGenerator()).getNextContainerId();
      mBlockContainerIdGenerator.setNextContainerId((mJournaledNextContainerId));
    } else if (entry.hasBlockInfo()) {
      BlockInfoEntry blockInfoEntry = entry.getBlockInfo();
      if (mBlocks.containsKey(blockInfoEntry.getBlockId())) {
        // Update the existing block info.
        MasterBlockInfo blockInfo = mBlocks.get(blockInfoEntry.getBlockId());
        blockInfo.updateLength(blockInfoEntry.getLength());
      } else {
        mBlocks.put(blockInfoEntry.getBlockId(), new MasterBlockInfo(blockInfoEntry.getBlockId(),
            blockInfoEntry.getLength()));
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    final Iterator<MasterBlockInfo> it = mBlocks.values().iterator();
    Iterator<JournalEntry> blockIterator = new Iterator<JournalEntry>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public JournalEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        MasterBlockInfo info = it.next();
        BlockInfoEntry blockInfoEntry =
            BlockInfoEntry.newBuilder().setBlockId(info.getBlockId())
                .setLength(info.getLength()).build();
        return JournalEntry.newBuilder().setBlockInfo(blockInfoEntry).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("BlockMaster#Iterator#remove is not supported.");
      }
    };

    return Iterators
        .concat(CommonUtils.singleElementIterator(getContainerIdJournalEntry()), blockIterator);
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    mGlobalStorageTierAssoc = new MasterStorageTierAssoc();
    if (isLeader) {
      mLostWorkerDetectionService = getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_WORKER_DETECTION, new LostWorkerDetectionHeartbeatExecutor(),
          Configuration.getInt(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
    }
    // ALLUXIO CS ADD
    if (mCapabilityEnabled) {
      mCapabilityKeyManager =
          new alluxio.master.security.capability.CapabilityKeyManager(
              mCapabilityKeyLifetimeMs, this);
    }
    // ALLUXIO CS END
  }

||||||| merged common ancestors
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class BlockMaster extends AbstractMaster implements ContainerIdGenerable {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMaster.class);

  /**
   * The number of container ids to 'reserve' before having to journal container id state. This
   * allows the master to return container ids within the reservation, without having to write to
   * the journal.
   */
  private static final long CONTAINER_ID_RESERVATION_SIZE = 1000;

  // Worker metadata management.
  private static final IndexDefinition<MasterWorkerInfo> ID_INDEX =
      new IndexDefinition<MasterWorkerInfo>(true) {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<MasterWorkerInfo> ADDRESS_INDEX =
      new IndexDefinition<MasterWorkerInfo>(true) {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getWorkerAddress();
        }
      };

  /**
   * Concurrency and locking in the BlockMaster
   *
   * The block master uses concurrent data structures to allow non-conflicting concurrent access.
   * This means each piece of metadata should be locked individually. There are two types of
   * metadata in the {@link BlockMaster}; {@link MasterBlockInfo} and {@link MasterWorkerInfo}.
   * Individual objects must be locked before modifying the object, or reading a modifiable field
   * of an object. This will protect the internal integrity of the metadata object.
   *
   * Lock ordering must be preserved in order to prevent deadlock. If both a worker and block
   * metadata must be locked at the same time, the worker metadata ({@link MasterWorkerInfo})
   * must be locked before the block metadata ({@link MasterBlockInfo}).
   *
   * It should not be the case that multiple worker metadata must be locked at the same time, or
   * multiple block metadata must be locked at the same time. Operations involving different
   * workers or different blocks should be able to be performed independently.
   */

  // Block metadata management.
  /** Blocks on all workers, including active and lost blocks. This state must be journaled. */
  private final ConcurrentHashMapV8<Long, MasterBlockInfo> mBlocks =
      new ConcurrentHashMapV8<>(8192, 0.90f, 64);
  /** Keeps track of blocks which are no longer in Alluxio storage. */
  private final ConcurrentHashSet<Long> mLostBlocks = new ConcurrentHashSet<>(64, 0.90f, 64);

  /** This state must be journaled. */
  @GuardedBy("itself")
  private final BlockContainerIdGenerator mBlockContainerIdGenerator =
      new BlockContainerIdGenerator();

  /**
   * Mapping between all possible storage level aliases and their ordinal position. This mapping
   * forms a total ordering on all storage level aliases in the system, and must be consistent
   * across masters.
   */
  private StorageTierAssoc mGlobalStorageTierAssoc;

  /** Keeps track of workers which are in communication with the master. */
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);
  /** Keeps track of workers which are no longer in communication with the master. */
  private final IndexedSet<MasterWorkerInfo> mLostWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);

  /**
   * The service that detects lost worker nodes, and tries to restart the failed workers.
   * We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLostWorkerDetectionService;

  /** The value of the 'next container id' last journaled. */
  @GuardedBy("mBlockContainerIdGenerator")
  private long mJournaledNextContainerId = 0;

  /**
   * Creates a new instance of {@link BlockMaster}.
   *
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  BlockMaster(JournalFactory journalFactory) {
    this(journalFactory, new SystemClock(), ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.BLOCK_MASTER_NAME, 2));
  }

  /**
   * Creates a new instance of {@link BlockMaster}.
   *
   * @param journalFactory the factory for the journal to use for tracking master operations
   * @param clock the clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  BlockMaster(JournalFactory journalFactory, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    super(journalFactory.create(Constants.BLOCK_MASTER_NAME), clock, executorServiceFactory);
    Metrics.registerGauges(this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME,
        new BlockMasterClientService.Processor<>(new BlockMasterClientServiceHandler(this)));
    services.put(Constants.BLOCK_MASTER_WORKER_SERVICE_NAME,
        new BlockMasterWorkerService.Processor<>(new BlockMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.BLOCK_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    if (entry.getSequenceNumber() == 0) {
      // This is the first journal entry, clear the master state.
      mBlocks.clear();
    }
    // TODO(gene): A better way to process entries besides a huge switch?
    if (entry.hasBlockContainerIdGenerator()) {
      mJournaledNextContainerId = (entry.getBlockContainerIdGenerator()).getNextContainerId();
      mBlockContainerIdGenerator.setNextContainerId((mJournaledNextContainerId));
    } else if (entry.hasBlockInfo()) {
      BlockInfoEntry blockInfoEntry = entry.getBlockInfo();
      if (mBlocks.containsKey(blockInfoEntry.getBlockId())) {
        // Update the existing block info.
        MasterBlockInfo blockInfo = mBlocks.get(blockInfoEntry.getBlockId());
        blockInfo.updateLength(blockInfoEntry.getLength());
      } else {
        mBlocks.put(blockInfoEntry.getBlockId(), new MasterBlockInfo(blockInfoEntry.getBlockId(),
            blockInfoEntry.getLength()));
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    final Iterator<MasterBlockInfo> it = mBlocks.values().iterator();
    Iterator<JournalEntry> blockIterator = new Iterator<JournalEntry>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public JournalEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        MasterBlockInfo info = it.next();
        BlockInfoEntry blockInfoEntry =
            BlockInfoEntry.newBuilder().setBlockId(info.getBlockId())
                .setLength(info.getLength()).build();
        return JournalEntry.newBuilder().setBlockInfo(blockInfoEntry).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("BlockMaster#Iterator#remove is not supported.");
      }
    };

    return Iterators
        .concat(CommonUtils.singleElementIterator(getContainerIdJournalEntry()), blockIterator);
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    mGlobalStorageTierAssoc = new MasterStorageTierAssoc();
    if (isLeader) {
      mLostWorkerDetectionService = getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_WORKER_DETECTION, new LostWorkerDetectionHeartbeatExecutor(),
          Configuration.getInt(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
    }
  }

=======
public interface BlockMaster extends Master, ContainerIdGenerable {
>>>>>>> 365297b45190d96a494f0bf248f3726531cce33e
  /**
   * @return the number of workers
   */
  int getWorkerCount();

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  List<WorkerInfo> getWorkerInfoList();

  /**
   * @return the total capacity (in bytes) on all tiers, on all workers of Alluxio
   */
  long getCapacityBytes();

  /**
   * @return the global storage tier mapping
   */
  StorageTierAssoc getGlobalStorageTierAssoc();

  /**
   * @return the total used bytes on all tiers, on all workers of Alluxio
   */
  long getUsedBytes();

  /**
   * @return a list of {@link WorkerInfo}s of lost workers
   */
  List<WorkerInfo> getLostWorkersInfoList();

  /**
   * Removes blocks from workers.
   *
   * @param blockIds a list of block ids to remove from Alluxio space
   * @param delete whether to delete blocks' metadata in Master
   */
  void removeBlocks(List<Long> blockIds, boolean delete);

  /**
   * Marks a block as committed on a specific worker.
   *
   * @param workerId the worker id committing the block
   * @param usedBytesOnTier the updated used bytes on the tier of the worker
   * @param tierAlias the alias of the storage tier where the worker is committing the block to
   * @param blockId the committing block id
   * @param length the length of the block
   * @throws NoWorkerException if the workerId is not active
   */
  // TODO(binfan): check the logic is correct or not when commitBlock is a retry
  void commitBlock(long workerId, long usedBytesOnTier, String tierAlias, long blockId, long
      length) throws NoWorkerException;

  /**
   * Marks a block as committed, but without a worker location. This means the block is only in ufs.
   *
   * @param blockId the id of the block to commit
   * @param length the length of the block
   */
  void commitBlockInUFS(long blockId, long length);

  /**
   * @param blockId the block id to get information for
   * @return the {@link BlockInfo} for the given block id
   * @throws BlockInfoException if the block info is not found
   */
  BlockInfo getBlockInfo(long blockId) throws BlockInfoException;

  /**
   * Retrieves information for the given list of block ids.
   *
   * @param blockIds A list of block ids to retrieve the information for
   * @return A list of {@link BlockInfo} objects corresponding to the input list of block ids. The
   *         list is in the same order as the input list
   */
  List<BlockInfo> getBlockInfoList(List<Long> blockIds);

  /**
   * @return the total bytes on each storage tier
   */
  Map<String, Long> getTotalBytesOnTiers();

  /**
   * @return the used bytes on each storage tier
   */
  Map<String, Long> getUsedBytesOnTiers();

  /**
   * Returns a worker id for the given worker, creating one if the worker is new.
   *
   * @param workerNetAddress the worker {@link WorkerNetAddress}
   * @return the worker id for this worker
   */
<<<<<<< HEAD
  public long getWorkerId(WorkerNetAddress workerNetAddress) {
    // TODO(gpang): Clone WorkerNetAddress in case thrift re-uses the object. Does thrift re-use it?
    MasterWorkerInfo existingWorker = mWorkers.getFirstByField(ADDRESS_INDEX, workerNetAddress);
    if (existingWorker != null) {
      // This worker address is already mapped to a worker id.
      long oldWorkerId = existingWorker.getId();
      LOG.warn("The worker {} already exists as id {}.", workerNetAddress, oldWorkerId);
      return oldWorkerId;
    }

    MasterWorkerInfo lostWorker = mLostWorkers.getFirstByField(ADDRESS_INDEX, workerNetAddress);
    if (lostWorker != null) {
      // this is one of the lost workers
      synchronized (lostWorker) {
        final long lostWorkerId = lostWorker.getId();
        LOG.warn("A lost worker {} has requested its old id {}.", workerNetAddress, lostWorkerId);

        // Update the timestamp of the worker before it is considered an active worker.
        lostWorker.updateLastUpdatedTimeMs();
        mWorkers.add(lostWorker);
        mLostWorkers.remove(lostWorker);
        return lostWorkerId;
      }
    }

    // Generate a new worker id.
    long workerId = IdUtils.getRandomNonNegativeLong();
    // ALLUXIO CS REPLACE
    // while (!mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress))) {
    //   workerId = IdUtils.getRandomNonNegativeLong();
    // }
    // ALLUXIO CS WITH
    // Make sure that the number of workers does not exceed the allowed maximum.
    synchronized (mWorkers) {
      if (!Boolean.parseBoolean(alluxio.LicenseConstants.LICENSE_CHECK_ENABLED)
          || mWorkers.size() < mMaxWorkers) {
        while (!mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress))) {
          workerId = IdUtils.getRandomNonNegativeLong();
        }
      } else {
        throw new RuntimeException("Maximum number of workers has been reached.");
      }
    }
    // ALLUXIO CS END

    LOG.info("getWorkerId(): WorkerNetAddress: {} id: {}", workerNetAddress, workerId);
    return workerId;
  }
||||||| merged common ancestors
  public long getWorkerId(WorkerNetAddress workerNetAddress) {
    // TODO(gpang): Clone WorkerNetAddress in case thrift re-uses the object. Does thrift re-use it?
    MasterWorkerInfo existingWorker = mWorkers.getFirstByField(ADDRESS_INDEX, workerNetAddress);
    if (existingWorker != null) {
      // This worker address is already mapped to a worker id.
      long oldWorkerId = existingWorker.getId();
      LOG.warn("The worker {} already exists as id {}.", workerNetAddress, oldWorkerId);
      return oldWorkerId;
    }

    MasterWorkerInfo lostWorker = mLostWorkers.getFirstByField(ADDRESS_INDEX, workerNetAddress);
    if (lostWorker != null) {
      // this is one of the lost workers
      synchronized (lostWorker) {
        final long lostWorkerId = lostWorker.getId();
        LOG.warn("A lost worker {} has requested its old id {}.", workerNetAddress, lostWorkerId);

        // Update the timestamp of the worker before it is considered an active worker.
        lostWorker.updateLastUpdatedTimeMs();
        mWorkers.add(lostWorker);
        mLostWorkers.remove(lostWorker);
        return lostWorkerId;
      }
    }

    // Generate a new worker id.
    long workerId = IdUtils.getRandomNonNegativeLong();
    while (!mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress))) {
      workerId = IdUtils.getRandomNonNegativeLong();
    }

    LOG.info("getWorkerId(): WorkerNetAddress: {} id: {}", workerNetAddress, workerId);
    return workerId;
  }
=======
  long getWorkerId(WorkerNetAddress workerNetAddress);
>>>>>>> 365297b45190d96a494f0bf248f3726531cce33e

  /**
   * Updates metadata when a worker registers with the master.
   *
   * @param workerId the worker id of the worker registering
   * @param storageTiers a list of storage tier aliases in order of their position in the worker's
   *        hierarchy
   * @param totalBytesOnTiers a mapping from storage tier alias to total bytes
   * @param usedBytesOnTiers a mapping from storage tier alias to the used byes
   * @param currentBlocksOnTiers a mapping from storage tier alias to a list of blocks
   * @throws NoWorkerException if workerId cannot be found
   */
  void workerRegister(long workerId, List<String> storageTiers,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers,
<<<<<<< HEAD
      Map<String, List<Long>> currentBlocksOnTiers) throws NoWorkerException {
    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
    if (worker == null) {
      throw new NoWorkerException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
    }

    // Gather all blocks on this worker.
    HashSet<Long> blocks = new HashSet<>();
    for (List<Long> blockIds : currentBlocksOnTiers.values()) {
      blocks.addAll(blockIds);
    }

    synchronized (worker) {
      worker.updateLastUpdatedTimeMs();
      // Detect any lost blocks on this worker.
      Set<Long> removedBlocks = worker.register(mGlobalStorageTierAssoc, storageTiers,
          totalBytesOnTiers, usedBytesOnTiers, blocks);
      processWorkerRemovedBlocks(worker, removedBlocks);
      processWorkerAddedBlocks(worker, currentBlocksOnTiers);
    }
    // ALLUXIO CS ADD

    if (mCapabilityEnabled) {
      mCapabilityKeyManager.scheduleNewKeyDistribution(worker.generateClientWorkerInfo());
    }
    // ALLUXIO CS END

    LOG.info("registerWorker(): {}", worker);
  }
||||||| merged common ancestors
      Map<String, List<Long>> currentBlocksOnTiers) throws NoWorkerException {
    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
    if (worker == null) {
      throw new NoWorkerException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
    }

    // Gather all blocks on this worker.
    HashSet<Long> blocks = new HashSet<>();
    for (List<Long> blockIds : currentBlocksOnTiers.values()) {
      blocks.addAll(blockIds);
    }

    synchronized (worker) {
      worker.updateLastUpdatedTimeMs();
      // Detect any lost blocks on this worker.
      Set<Long> removedBlocks = worker.register(mGlobalStorageTierAssoc, storageTiers,
          totalBytesOnTiers, usedBytesOnTiers, blocks);
      processWorkerRemovedBlocks(worker, removedBlocks);
      processWorkerAddedBlocks(worker, currentBlocksOnTiers);
    }

    LOG.info("registerWorker(): {}", worker);
  }
=======
      Map<String, List<Long>> currentBlocksOnTiers) throws NoWorkerException;
>>>>>>> 365297b45190d96a494f0bf248f3726531cce33e

  /**
   * Updates metadata when a worker periodically heartbeats with the master.
   *
   * @param workerId the worker id
   * @param usedBytesOnTiers a mapping from tier alias to the used bytes
   * @param removedBlockIds a list of block ids removed from this worker
   * @param addedBlocksOnTiers a mapping from tier alias to the added blocks
   * @return an optional command for the worker to execute
   */
  Command workerHeartbeat(long workerId, Map<String, Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<String, List<Long>> addedBlocksOnTiers);

  /**
   * @return the block ids of lost blocks in Alluxio
   */
  Set<Long> getLostBlocks();

  /**
   * Reports the ids of the blocks lost on workers.
   *
   * @param blockIds the ids of the lost blocks
   */
<<<<<<< HEAD
  public void reportLostBlocks(List<Long> blockIds) {
    mLostBlocks.addAll(blockIds);
  }

  // ALLUXIO CS ADD
  /**
   * @param maxWorkers the number of max workers to use
   */
  public void setMaxWorkers(int maxWorkers) {
    mMaxWorkers = maxWorkers;
  }

  /**
   * @return true if the capability is enabled, false otherwise
   */
  public boolean getCapabilityEnabled() {
    return mCapabilityEnabled;
  }

  /**
   * @return the capability life time
   */
  public long getCapabilityLifeTimeMs() {
    return mCapabilityLifetimeMs;
  }

  /**
   * @return the capability key manager
   */
  public alluxio.master.security.capability.CapabilityKeyManager getCapabilityKeyManager() {
    return mCapabilityKeyManager;
  }

  // ALLUXIO CS END
  /**
   * Lost worker periodic check.
   */
  private final class LostWorkerDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostWorkerDetectionHeartbeatExecutor}.
     */
    public LostWorkerDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat() {
      int masterWorkerTimeoutMs = Configuration.getInt(PropertyKey.MASTER_WORKER_TIMEOUT_MS);
      for (MasterWorkerInfo worker : mWorkers) {
        synchronized (worker) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.error("The worker {} timed out after {}ms without a heartbeat!", worker,
                lastUpdate);
            mLostWorkers.add(worker);
            mWorkers.remove(worker);
            processWorkerRemovedBlocks(worker, worker.getBlocks());
          }
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Class that contains metrics related to BlockMaster.
   */
  public static final class Metrics {
    public static final String CAPACITY_TOTAL = "CapacityTotal";
    public static final String CAPACITY_USED = "CapacityUsed";
    public static final String CAPACITY_FREE = "CapacityFree";
    public static final String WORKERS = "Workers";

    private static void registerGauges(final BlockMaster master) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(CAPACITY_TOTAL),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return master.getCapacityBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(CAPACITY_USED),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return master.getUsedBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(CAPACITY_FREE),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return master.getCapacityBytes() - master.getUsedBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(WORKERS),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getWorkerCount();
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }
||||||| merged common ancestors
  public void reportLostBlocks(List<Long> blockIds) {
    mLostBlocks.addAll(blockIds);
  }

  /**
   * Lost worker periodic check.
   */
  private final class LostWorkerDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostWorkerDetectionHeartbeatExecutor}.
     */
    public LostWorkerDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat() {
      int masterWorkerTimeoutMs = Configuration.getInt(PropertyKey.MASTER_WORKER_TIMEOUT_MS);
      for (MasterWorkerInfo worker : mWorkers) {
        synchronized (worker) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.error("The worker {} timed out after {}ms without a heartbeat!", worker,
                lastUpdate);
            mLostWorkers.add(worker);
            mWorkers.remove(worker);
            processWorkerRemovedBlocks(worker, worker.getBlocks());
          }
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Class that contains metrics related to BlockMaster.
   */
  public static final class Metrics {
    public static final String CAPACITY_TOTAL = "CapacityTotal";
    public static final String CAPACITY_USED = "CapacityUsed";
    public static final String CAPACITY_FREE = "CapacityFree";
    public static final String WORKERS = "Workers";

    private static void registerGauges(final BlockMaster master) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(CAPACITY_TOTAL),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return master.getCapacityBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(CAPACITY_USED),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return master.getUsedBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(CAPACITY_FREE),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return master.getCapacityBytes() - master.getUsedBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(WORKERS),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getWorkerCount();
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }
=======
  void reportLostBlocks(List<Long> blockIds);
>>>>>>> 365297b45190d96a494f0bf248f3726531cce33e
}
