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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
<<<<<<< HEAD
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterRegistry;
import alluxio.master.ProtobufUtils;
import alluxio.master.block.BlockId;
import alluxio.master.block.BlockMaster;
// ALLUXIO CS REMOVE
// import alluxio.master.file.async.AsyncPersistHandler;
// ALLUXIO CS END
||||||| merged common ancestors
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterRegistry;
import alluxio.master.ProtobufUtils;
import alluxio.master.block.BlockId;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.async.AsyncPersistHandler;
=======
import alluxio.master.Master;
>>>>>>> fae727c2236d0c5dfe53c04006dd70509cb38b94
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.thrift.FileSystemCommand;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.TtlAction;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The interface of file system master.
 */
<<<<<<< HEAD
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class FileSystemMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMaster.class);
  // ALLUXIO CS REPLACE
  // private static final Set<Class<?>> DEPS = ImmutableSet.<Class<?>>of(BlockMaster.class);
  // ALLUXIO CS WITH
  private static final Set<Class<?>> DEPS =
      ImmutableSet.<Class<?>>of(BlockMaster.class, alluxio.master.privilege.PrivilegeMaster.class);
  // ALLUXIO CS END

  /**
   * Locking in the FileSystemMaster
   *
   * Individual paths are locked in the inode tree. In order to read or write any inode, the path
   * must be locked. A path is locked via one of the lock methods in {@link InodeTree}, such as
   * {@link InodeTree#lockInodePath(AlluxioURI, InodeTree.LockMode)} or
   * {@link InodeTree#lockFullInodePath(AlluxioURI, InodeTree.LockMode)}. These lock methods return
   * an {@link LockedInodePath}, which represents a locked path of inodes. These locked paths
   * ({@link LockedInodePath}) must be unlocked. In order to ensure a locked
   * {@link LockedInodePath} is always unlocked, the following paradigm is recommended:
   *
   * <p><blockquote><pre>
   *    try (LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
   *      ...
   *    }
   * </pre></blockquote>
   *
   * When locking a path in the inode tree, it is possible that other concurrent operations have
   * modified the inode tree while a thread is waiting to acquire a lock on the inode. Lock
   * acquisitions throw {@link InvalidPathException} to indicate that the inode structure is no
   * longer consistent with what the caller original expected, for example if the inode
   * previously obtained at /pathA has been renamed to /pathB during the wait for the inode lock.
   * Methods which specifically act on a path will propagate this exception to the caller, while
   * methods which iterate over child nodes can safely ignore the exception and treat the inode
   * as no longer a child.
   *
   * Journaling in the FileSystemMaster
   *
   * Any changes to file system metadata that need to survive system restarts and / or failures
   * should be journaled. The intent to journal and the actual writing of the journal is decoupled
   * so that operations are not holding metadata locks waiting on the journal IO. In particular,
   * file system operations are expected to create a {@link JournalContext} resource, use it
   * throughout the lifetime of an operation to collect journal events through
   * {@link #appendJournalEntry(JournalEntry, JournalContext)}, and then close the resource, which
   * will persist the journal events. In order to ensure the journal events are always persisted,
   * the following paradigm is recommended:
   *
   * <p><blockquote><pre>
   *    try (JournalContext journalContext = createJournalContext()) {
   *      ...
   *    }
   * </pre></blockquote>
   *
   * NOTE: Because resources are released in the opposite order they are acquired, the
   * {@link JournalContext} resources should be always created before any {@link LockedInodePath}
   * resources to avoid holding an inode path lock while waiting for journal IO.
   *
   * Method Conventions in the FileSystemMaster
   *
   * All of the flow of the FileSystemMaster follow a convention. There are essentially 4 main
   * types of methods:
   *   (A) public api methods
   *   (B) private (or package private) methods that journal
   *   (C) private (or package private) internal methods
   *   (D) private FromEntry methods used to replay entries from the journal
   *
   * (A) public api methods:
   * These methods are public and are accessed by the RPC and REST APIs. These methods lock all
   * the required paths, and also perform all permission checking.
   * (A) cannot call (A)
   * (A) can call (B)
   * (A) can call (C)
   * (A) cannot call (D)
   *
   * (B) private (or package private) methods that journal:
   * These methods perform the work from the public apis, and also asynchronously write to the
   * journal (for write operations). The names of these methods are suffixed with "AndJournal".
   * (B) cannot call (A)
   * (B) can call (B)
   * (B) can call (C)
   * (B) cannot call (D)
   *
   * (C) private (or package private) internal methods:
   * These methods perform the rest of the work, and do not do any journaling. The names of these
   * methods are suffixed by "Internal".
   * (C) cannot call (A)
   * (C) cannot call (B)
   * (C) can call (C)
   * (C) cannot call (D)
   *
   * (D) private FromEntry methods used to replay entries from the journal:
   * These methods are used to replay entries from reading the journal. This is done on start, as
   * well as for standby masters.
   * (D) cannot call (A)
   * (D) cannot call (B)
   * (D) can call (C)
   * (D) cannot call (D)
   */

  /** Handle to the block master. */
  private final BlockMaster mBlockMaster;

  /** This manages the file system inode structure. This must be journaled. */
  private final InodeTree mInodeTree;

  /** This manages the file system mount points. */
  private final MountTable mMountTable;

  /** This maintains inodes with ttl set, for the for the ttl checker service to use. */
  private final TtlBucketList mTtlBuckets = new TtlBucketList();

  /** This generates unique directory ids. This must be journaled. */
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;

  /** This checks user permissions on different operations. */
  private final PermissionChecker mPermissionChecker;

  /** List of paths to always keep in memory. */
  private final PrefixList mWhitelist;

  // ALLUXIO CS REPLACE
  // /** The handler for async persistence. */
  // private final AsyncPersistHandler mAsyncPersistHandler;
  // ALLUXIO CS WITH
  /** Map from file IDs to persist requests. */
  private final Map<Long, PersistRequest> mPersistRequests;

  /** Map from file IDs to persist jobs. */
  private final Map<Long, PersistJob> mPersistJobs;

  /** This checks user privileges on privileged operations. */
  private final alluxio.master.privilege.PrivilegeChecker mPrivilegeChecker;
  // ALLUXIO CS END

  /**
   * The service that checks for inode files with ttl set. We store it here so that it can be
   * accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mTtlCheckerService;

  /**
   * The service that detects lost files. We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLostFilesDetectionService;

  // ALLUXIO CS ADD
  /**
   * Services used for asynchronous persistence.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mPersistenceSchedulerService;
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mPersistenceCheckerService;

  /**
   * The service that checks replication level for blocks. We store it here so that it can be
   * accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mReplicationCheckService;

  // ALLUXIO CS END
  private Future<List<AlluxioURI>> mStartupConsistencyCheck;

  /**
   * Creates a new instance of {@link FileSystemMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  public FileSystemMaster(MasterRegistry registry, JournalFactory journalFactory) {
    // ALLUXIO CS REPLACE
    // this(registry, journalFactory, ExecutorServiceFactories
    //     .fixedThreadPoolExecutorServiceFactory(Constants.FILE_SYSTEM_MASTER_NAME, 3));
    // ALLUXIO CS WITH
    this(registry, journalFactory, ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.FILE_SYSTEM_MASTER_NAME, 7));
    // ALLUXIO CS END
  }

  /**
   * Creates a new instance of {@link FileSystemMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  public FileSystemMaster(MasterRegistry registry, JournalFactory journalFactory,
      ExecutorServiceFactory executorServiceFactory) {
    super(journalFactory.create(Constants.FILE_SYSTEM_MASTER_NAME), new SystemClock(),
        executorServiceFactory);

    mBlockMaster = registry.get(BlockMaster.class);
    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    mMountTable = new MountTable();
    mInodeTree = new InodeTree(mBlockMaster, mDirectoryIdGenerator, mMountTable);

    // TODO(gene): Handle default config value for whitelist.
    mWhitelist = new PrefixList(Configuration.getList(PropertyKey.MASTER_WHITELIST, ","));

    // ALLUXIO CS REPLACE
    // mAsyncPersistHandler = AsyncPersistHandler.Factory.create(new FileSystemMasterView(this));
    // ALLUXIO CS WITH
    mPersistRequests = new java.util.concurrent.ConcurrentHashMap<>();
    mPersistJobs = new java.util.concurrent.ConcurrentHashMap<>();
    mPrivilegeChecker = new alluxio.master.privilege.PrivilegeChecker(
        registry.get(alluxio.master.privilege.PrivilegeMaster.class));
    // ALLUXIO CS END
    mPermissionChecker = new PermissionChecker(mInodeTree);

    registry.add(FileSystemMaster.class, this);
    Metrics.registerGauges(this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME,
        // ALLUXIO CS REPLACE
        // new FileSystemMasterClientService.Processor<>(
        //     new FileSystemMasterClientServiceHandler(this)));
        // ALLUXIO CS WITH
        new FileSystemMasterClientService.Processor<>(
            new FileSystemMasterClientServiceHandler(this, mPrivilegeChecker)));
        // ALLUXIO CS END
    services.put(Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME,
        new FileSystemMasterWorkerService.Processor<>(
            new FileSystemMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.FILE_SYSTEM_MASTER_NAME;
  }

  @Override
  public Set<Class<?>> getDependencies() {
    return DEPS;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    if (entry.hasInodeFile()) {
      mInodeTree.addInodeFileFromJournal(entry.getInodeFile());
      // Add the file to TTL buckets, the insert automatically rejects files w/ Constants.NO_TTL
      InodeFileEntry inodeFileEntry = entry.getInodeFile();
      if (inodeFileEntry.hasTtl()) {
        mTtlBuckets.insert(InodeFile.fromJournalEntry(inodeFileEntry));
      }
    } else if (entry.hasInodeDirectory()) {
      try {
        // Add the directory to TTL buckets, the insert automatically rejects directory
        // w/ Constants.NO_TTL
        InodeDirectoryEntry inodeDirectoryEntry = entry.getInodeDirectory();
        if (inodeDirectoryEntry.hasTtl()) {
          mTtlBuckets.insert(InodeDirectory.fromJournalEntry(inodeDirectoryEntry));
        }
        mInodeTree.addInodeDirectoryFromJournal(entry.getInodeDirectory());
      } catch (AccessControlException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasInodeLastModificationTime()) {
      InodeLastModificationTimeEntry modTimeEntry = entry.getInodeLastModificationTime();
      try (LockedInodePath inodePath = mInodeTree
          .lockFullInodePath(modTimeEntry.getId(), InodeTree.LockMode.WRITE)) {
        inodePath.getInode()
            .setLastModificationTimeMs(modTimeEntry.getLastModificationTimeMs(), true);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasPersistDirectory()) {
      PersistDirectoryEntry typedEntry = entry.getPersistDirectory();
      try (LockedInodePath inodePath = mInodeTree
          .lockFullInodePath(typedEntry.getId(), InodeTree.LockMode.WRITE)) {
        inodePath.getInode().setPersistenceState(PersistenceState.PERSISTED);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasCompleteFile()) {
      try {
        completeFileFromEntry(entry.getCompleteFile());
      } catch (InvalidPathException | InvalidFileSizeException | FileAlreadyCompletedException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasSetAttribute()) {
      try {
        setAttributeFromEntry(entry.getSetAttribute());
      } catch (AccessControlException | FileDoesNotExistException | InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasDeleteFile()) {
      deleteFromEntry(entry.getDeleteFile());
    } else if (entry.hasRename()) {
      renameFromEntry(entry.getRename());
    } else if (entry.hasInodeDirectoryIdGenerator()) {
      mDirectoryIdGenerator.initFromJournalEntry(entry.getInodeDirectoryIdGenerator());
    } else if (entry.hasReinitializeFile()) {
      resetBlockFileFromEntry(entry.getReinitializeFile());
    } else if (entry.hasAddMountPoint()) {
      try {
        mountFromEntry(entry.getAddMountPoint());
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasDeleteMountPoint()) {
      try {
        unmountFromEntry(entry.getDeleteMountPoint());
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasAsyncPersistRequest()) {
      try {
        long fileId = (entry.getAsyncPersistRequest()).getFileId();
        try (LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
          scheduleAsyncPersistenceInternal(inodePath);
        }
        // ALLUXIO CS REMOVE
        // // NOTE: persistence is asynchronous so there is no guarantee the path will still exist
        // mAsyncPersistHandler.scheduleAsyncPersistence(getPath(fileId));
        // ALLUXIO CS END
      } catch (AlluxioException e) {
        // It's possible that rescheduling the async persist calls fails, because the blocks may no
        // longer be in the memory
        LOG.error(e.getMessage());
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    mInodeTree.streamToJournalCheckpoint(outputStream);
    outputStream.write(mDirectoryIdGenerator.toJournalEntry());
    // The mount table should be written to the checkpoint after the inodes are written, so that
    // when replaying the checkpoint, the inodes exist before mount entries. Replaying a mount
    // entry traverses the inode tree.
    mMountTable.streamToJournalCheckpoint(outputStream);
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    if (isLeader) {
      // Only initialize root when isLeader because when initializing root, BlockMaster needs to
      // write journal entry, if it is not leader, BlockMaster won't have a writable journal.
      // If it is standby, it should be able to load the inode tree from leader's checkpoint.
      mInodeTree.initializeRoot(SecurityUtils.getOwnerFromLoginModule(),
          SecurityUtils.getGroupFromLoginModule(), Mode.createFullAccess().applyDirectoryUMask());
      String defaultUFS = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
      try {
        mMountTable.add(new AlluxioURI(MountTable.ROOT), new AlluxioURI(defaultUFS),
            MountOptions.defaults().setShared(
                UnderFileSystemUtils.isObjectStorage(defaultUFS) && Configuration
                    .getBoolean(PropertyKey.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY)));
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        throw new IOException("Failed to mount the default UFS " + defaultUFS);
      }
    }
    // Call super.start after mInodeTree is initialized because mInodeTree is needed to write
    // a journal entry during super.start. Call super.start before calling
    // getExecutorService() because the super.start initializes the executor service.
    super.start(isLeader);
    if (isLeader) {
      mTtlCheckerService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_TTL_CHECK, new MasterInodeTtlCheckExecutor(),
              Configuration.getInt(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS)));
      mLostFilesDetectionService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_LOST_FILES_DETECTION,
              new LostFilesDetectionHeartbeatExecutor(),
              Configuration.getInt(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
      // ALLUXIO CS ADD
      mReplicationCheckService = getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_REPLICATION_CHECK,
          new alluxio.master.file.replication.ReplicationChecker(mInodeTree, mBlockMaster),
          Configuration.getInt(PropertyKey.MASTER_REPLICATION_CHECK_INTERVAL_MS)));
      mPersistenceSchedulerService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER,
              new PersistenceScheduler(),
              Configuration.getInt(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS)));
      mPersistenceCheckerService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_PERSISTENCE_CHECKER,
              new PersistenceChecker(),
              Configuration.getInt(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS)));
      // ALLUXIO CS END
      if (Configuration.getBoolean(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)) {
        mStartupConsistencyCheck = getExecutorService().submit(new Callable<List<AlluxioURI>>() {
          @Override
          public List<AlluxioURI> call() throws Exception {
            return startupCheckConsistency(ExecutorServiceFactories
                .fixedThreadPoolExecutorServiceFactory("startup-consistency-check", 32).create());
          }
        });
      }
    }
  }

  /**
   * Checks the consistency of the root in a multi-threaded and incremental fashion. This method
   * will only READ lock the directories and files actively being checked and release them after the
   * check on the file / directory is complete.
   *
   * @return a list of paths in Alluxio which are not consistent with the under storage
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws IOException if an error occurs interacting with the under storage
   */
  private List<AlluxioURI> startupCheckConsistency(final ExecutorService service)
      throws InterruptedException, IOException {
    /** A marker {@link StartupConsistencyChecker}s add to the queue to signal completion */
    final long completionMarker = -1;
    /** A shared queue of directories which have yet to be checked */
    final BlockingQueue<Long> dirsToCheck = new LinkedBlockingQueue<>();

    /**
     * A {@link Callable} which checks the consistency of a directory.
     */
    final class StartupConsistencyChecker implements Callable<List<AlluxioURI>> {
      /** The path to check, guaranteed to be a directory in Alluxio. */
      private final Long mFileId;

      /**
       * Creates a new callable which checks the consistency of a directory.
       * @param fileId the path to check
       */
      private StartupConsistencyChecker(Long fileId) {
        mFileId = fileId;
      }

      /**
       * Checks the consistency of the directory and all immediate children which are files. All
       * immediate children which are directories are added to the shared queue of directories to
       * check. The parent directory is READ locked during the entire call while the children are
       * READ locked only during the consistency check of the children files.
       *
       * @return a list of inconsistent uris
       * @throws IOException if an error occurs interacting with the under storage
       */
      @Override
      public List<AlluxioURI> call() throws IOException {
        List<AlluxioURI> inconsistentUris = new ArrayList<>();
        try (LockedInodePath dir = mInodeTree.lockFullInodePath(mFileId, InodeTree.LockMode.READ)) {
          Inode parentInode = dir.getInode();
          AlluxioURI parentUri = dir.getUri();
          if (!checkConsistencyInternal(parentInode, parentUri)) {
            inconsistentUris.add(parentUri);
          }
          for (Inode childInode : ((InodeDirectory) parentInode).getChildren()) {
            try {
              childInode.lockReadAndCheckParent(parentInode);
            } catch (InvalidPathException e) {
              // This should be safe, continue.
              LOG.debug("Error during startup check consistency, ignoring and continuing.", e);
              continue;
            }
            try {
              AlluxioURI childUri = parentUri.join(childInode.getName());
              if (childInode.isDirectory()) {
                dirsToCheck.add(childInode.getId());
              } else {
                if (!checkConsistencyInternal(childInode, childUri)) {
                  inconsistentUris.add(childUri);
                }
              }
            } finally {
              childInode.unlockRead();
            }
          }
        } catch (FileDoesNotExistException e) {
          // This should be safe, continue.
          LOG.debug("A file scheduled for consistency check was deleted before the check.");
        } catch (InvalidPathException e) {
          // This should not happen.
          LOG.error("An invalid path was discovered during the consistency check, skipping.", e);
        }
        dirsToCheck.add(completionMarker);
        return inconsistentUris;
      }
    }

    // Add the root to the directories to check.
    dirsToCheck.add(mInodeTree.getRoot().getId());
    List<Future<List<AlluxioURI>>> results = new ArrayList<>();
    // Tracks how many checkers have been started.
    long started = 0;
    // Tracks how many checkers have completed.
    long completed = 0;
    do {
      Long fileId = dirsToCheck.take();
      if (fileId == completionMarker) { // A thread signaled completion.
        completed++;
      } else { // A new directory needs to be checked.
        StartupConsistencyChecker checker = new StartupConsistencyChecker(fileId);
        results.add(service.submit(checker));
        started++;
      }
    } while (started != completed);

    // Return the total set of inconsistent paths discovered.
    List<AlluxioURI> inconsistentUris = new ArrayList<>();
    for (Future<List<AlluxioURI>> result : results) {
      try {
        inconsistentUris.addAll(result.get());
      } catch (Exception e) {
        // This shouldn't happen, all futures should be complete.
        Throwables.propagate(e);
      }
    }
    service.shutdown();
    return inconsistentUris;
  }

  /**
   * Class to represent the status and result of the startup consistency check.
   */
  public static final class StartupConsistencyCheck {
    /**
     * Status of the check.
     */
    public enum Status {
      COMPLETE,
      DISABLED,
      FAILED,
      RUNNING
    }

    /**
     * @param inconsistentUris the uris which are inconsistent with the underlying storage
     * @return a result set to the complete status
     */
    public static StartupConsistencyCheck complete(List<AlluxioURI> inconsistentUris) {
      return new StartupConsistencyCheck(Status.COMPLETE, inconsistentUris);
    }

    /**
     * @return a result set to the disabled status
     */
    public static StartupConsistencyCheck disabled() {
      return new StartupConsistencyCheck(Status.DISABLED, null);
    }

    /**
     * @return a result set to the failed status
     */
    public static StartupConsistencyCheck failed() {
      return new StartupConsistencyCheck(Status.FAILED, null);
    }

    /**
     * @return a result set to the running status
     */
    public static StartupConsistencyCheck running() {
      return new StartupConsistencyCheck(Status.RUNNING, null);
    }

    private Status mStatus;
    private List<AlluxioURI> mInconsistentUris;

    /**
     * Create a new startup consistency check result.
     *
     * @param status the state of the check
     * @param inconsistentUris the uris which are inconsistent with the underlying storage
     */
    private StartupConsistencyCheck(Status status, List<AlluxioURI> inconsistentUris) {
      mStatus = status;
      mInconsistentUris = inconsistentUris;
    }

    /**
     * @return the status of the check
     */
    public Status getStatus() {
      return mStatus;
    }

    /**
     * @return the uris which are inconsistent with the underlying storage
     */
    public List<AlluxioURI> getInconsistentUris() {
      return mInconsistentUris;
    }
  }

||||||| merged common ancestors
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class FileSystemMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMaster.class);
  private static final Set<Class<?>> DEPS = ImmutableSet.<Class<?>>of(BlockMaster.class);

  /**
   * Locking in the FileSystemMaster
   *
   * Individual paths are locked in the inode tree. In order to read or write any inode, the path
   * must be locked. A path is locked via one of the lock methods in {@link InodeTree}, such as
   * {@link InodeTree#lockInodePath(AlluxioURI, InodeTree.LockMode)} or
   * {@link InodeTree#lockFullInodePath(AlluxioURI, InodeTree.LockMode)}. These lock methods return
   * an {@link LockedInodePath}, which represents a locked path of inodes. These locked paths
   * ({@link LockedInodePath}) must be unlocked. In order to ensure a locked
   * {@link LockedInodePath} is always unlocked, the following paradigm is recommended:
   *
   * <p><blockquote><pre>
   *    try (LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
   *      ...
   *    }
   * </pre></blockquote>
   *
   * When locking a path in the inode tree, it is possible that other concurrent operations have
   * modified the inode tree while a thread is waiting to acquire a lock on the inode. Lock
   * acquisitions throw {@link InvalidPathException} to indicate that the inode structure is no
   * longer consistent with what the caller original expected, for example if the inode
   * previously obtained at /pathA has been renamed to /pathB during the wait for the inode lock.
   * Methods which specifically act on a path will propagate this exception to the caller, while
   * methods which iterate over child nodes can safely ignore the exception and treat the inode
   * as no longer a child.
   *
   * Journaling in the FileSystemMaster
   *
   * Any changes to file system metadata that need to survive system restarts and / or failures
   * should be journaled. The intent to journal and the actual writing of the journal is decoupled
   * so that operations are not holding metadata locks waiting on the journal IO. In particular,
   * file system operations are expected to create a {@link JournalContext} resource, use it
   * throughout the lifetime of an operation to collect journal events through
   * {@link #appendJournalEntry(JournalEntry, JournalContext)}, and then close the resource, which
   * will persist the journal events. In order to ensure the journal events are always persisted,
   * the following paradigm is recommended:
   *
   * <p><blockquote><pre>
   *    try (JournalContext journalContext = createJournalContext()) {
   *      ...
   *    }
   * </pre></blockquote>
   *
   * NOTE: Because resources are released in the opposite order they are acquired, the
   * {@link JournalContext} resources should be always created before any {@link LockedInodePath}
   * resources to avoid holding an inode path lock while waiting for journal IO.
   *
   * Method Conventions in the FileSystemMaster
   *
   * All of the flow of the FileSystemMaster follow a convention. There are essentially 4 main
   * types of methods:
   *   (A) public api methods
   *   (B) private (or package private) methods that journal
   *   (C) private (or package private) internal methods
   *   (D) private FromEntry methods used to replay entries from the journal
   *
   * (A) public api methods:
   * These methods are public and are accessed by the RPC and REST APIs. These methods lock all
   * the required paths, and also perform all permission checking.
   * (A) cannot call (A)
   * (A) can call (B)
   * (A) can call (C)
   * (A) cannot call (D)
   *
   * (B) private (or package private) methods that journal:
   * These methods perform the work from the public apis, and also asynchronously write to the
   * journal (for write operations). The names of these methods are suffixed with "AndJournal".
   * (B) cannot call (A)
   * (B) can call (B)
   * (B) can call (C)
   * (B) cannot call (D)
   *
   * (C) private (or package private) internal methods:
   * These methods perform the rest of the work, and do not do any journaling. The names of these
   * methods are suffixed by "Internal".
   * (C) cannot call (A)
   * (C) cannot call (B)
   * (C) can call (C)
   * (C) cannot call (D)
   *
   * (D) private FromEntry methods used to replay entries from the journal:
   * These methods are used to replay entries from reading the journal. This is done on start, as
   * well as for standby masters.
   * (D) cannot call (A)
   * (D) cannot call (B)
   * (D) can call (C)
   * (D) cannot call (D)
   */

  /** Handle to the block master. */
  private final BlockMaster mBlockMaster;

  /** This manages the file system inode structure. This must be journaled. */
  private final InodeTree mInodeTree;

  /** This manages the file system mount points. */
  private final MountTable mMountTable;

  /** This maintains inodes with ttl set, for the for the ttl checker service to use. */
  private final TtlBucketList mTtlBuckets = new TtlBucketList();

  /** This generates unique directory ids. This must be journaled. */
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;

  /** This checks user permissions on different operations. */
  private final PermissionChecker mPermissionChecker;

  /** List of paths to always keep in memory. */
  private final PrefixList mWhitelist;

  /** The handler for async persistence. */
  private final AsyncPersistHandler mAsyncPersistHandler;

  /**
   * The service that checks for inode files with ttl set. We store it here so that it can be
   * accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mTtlCheckerService;

  /**
   * The service that detects lost files. We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLostFilesDetectionService;

  private Future<List<AlluxioURI>> mStartupConsistencyCheck;

  /**
   * Creates a new instance of {@link FileSystemMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  public FileSystemMaster(MasterRegistry registry, JournalFactory journalFactory) {
    this(registry, journalFactory, ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.FILE_SYSTEM_MASTER_NAME, 3));
  }

  /**
   * Creates a new instance of {@link FileSystemMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  public FileSystemMaster(MasterRegistry registry, JournalFactory journalFactory,
      ExecutorServiceFactory executorServiceFactory) {
    super(journalFactory.create(Constants.FILE_SYSTEM_MASTER_NAME), new SystemClock(),
        executorServiceFactory);

    mBlockMaster = registry.get(BlockMaster.class);
    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    mMountTable = new MountTable();
    mInodeTree = new InodeTree(mBlockMaster, mDirectoryIdGenerator, mMountTable);

    // TODO(gene): Handle default config value for whitelist.
    mWhitelist = new PrefixList(Configuration.getList(PropertyKey.MASTER_WHITELIST, ","));

    mAsyncPersistHandler = AsyncPersistHandler.Factory.create(new FileSystemMasterView(this));
    mPermissionChecker = new PermissionChecker(mInodeTree);

    registry.add(FileSystemMaster.class, this);
    Metrics.registerGauges(this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME,
        new FileSystemMasterClientService.Processor<>(
            new FileSystemMasterClientServiceHandler(this)));
    services.put(Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME,
        new FileSystemMasterWorkerService.Processor<>(
            new FileSystemMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.FILE_SYSTEM_MASTER_NAME;
  }

  @Override
  public Set<Class<?>> getDependencies() {
    return DEPS;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    if (entry.hasInodeFile()) {
      mInodeTree.addInodeFileFromJournal(entry.getInodeFile());
      // Add the file to TTL buckets, the insert automatically rejects files w/ Constants.NO_TTL
      InodeFileEntry inodeFileEntry = entry.getInodeFile();
      if (inodeFileEntry.hasTtl()) {
        mTtlBuckets.insert(InodeFile.fromJournalEntry(inodeFileEntry));
      }
    } else if (entry.hasInodeDirectory()) {
      try {
        // Add the directory to TTL buckets, the insert automatically rejects directory
        // w/ Constants.NO_TTL
        InodeDirectoryEntry inodeDirectoryEntry = entry.getInodeDirectory();
        if (inodeDirectoryEntry.hasTtl()) {
          mTtlBuckets.insert(InodeDirectory.fromJournalEntry(inodeDirectoryEntry));
        }
        mInodeTree.addInodeDirectoryFromJournal(entry.getInodeDirectory());
      } catch (AccessControlException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasInodeLastModificationTime()) {
      InodeLastModificationTimeEntry modTimeEntry = entry.getInodeLastModificationTime();
      try (LockedInodePath inodePath = mInodeTree
          .lockFullInodePath(modTimeEntry.getId(), InodeTree.LockMode.WRITE)) {
        inodePath.getInode()
            .setLastModificationTimeMs(modTimeEntry.getLastModificationTimeMs(), true);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasPersistDirectory()) {
      PersistDirectoryEntry typedEntry = entry.getPersistDirectory();
      try (LockedInodePath inodePath = mInodeTree
          .lockFullInodePath(typedEntry.getId(), InodeTree.LockMode.WRITE)) {
        inodePath.getInode().setPersistenceState(PersistenceState.PERSISTED);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasCompleteFile()) {
      try {
        completeFileFromEntry(entry.getCompleteFile());
      } catch (InvalidPathException | InvalidFileSizeException | FileAlreadyCompletedException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasSetAttribute()) {
      try {
        setAttributeFromEntry(entry.getSetAttribute());
      } catch (AccessControlException | FileDoesNotExistException | InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasDeleteFile()) {
      deleteFromEntry(entry.getDeleteFile());
    } else if (entry.hasRename()) {
      renameFromEntry(entry.getRename());
    } else if (entry.hasInodeDirectoryIdGenerator()) {
      mDirectoryIdGenerator.initFromJournalEntry(entry.getInodeDirectoryIdGenerator());
    } else if (entry.hasReinitializeFile()) {
      resetBlockFileFromEntry(entry.getReinitializeFile());
    } else if (entry.hasAddMountPoint()) {
      try {
        mountFromEntry(entry.getAddMountPoint());
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasDeleteMountPoint()) {
      try {
        unmountFromEntry(entry.getDeleteMountPoint());
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasAsyncPersistRequest()) {
      try {
        long fileId = (entry.getAsyncPersistRequest()).getFileId();
        try (LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
          scheduleAsyncPersistenceInternal(inodePath);
        }
        // NOTE: persistence is asynchronous so there is no guarantee the path will still exist
        mAsyncPersistHandler.scheduleAsyncPersistence(getPath(fileId));
      } catch (AlluxioException e) {
        // It's possible that rescheduling the async persist calls fails, because the blocks may no
        // longer be in the memory
        LOG.error(e.getMessage());
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    mInodeTree.streamToJournalCheckpoint(outputStream);
    outputStream.write(mDirectoryIdGenerator.toJournalEntry());
    // The mount table should be written to the checkpoint after the inodes are written, so that
    // when replaying the checkpoint, the inodes exist before mount entries. Replaying a mount
    // entry traverses the inode tree.
    mMountTable.streamToJournalCheckpoint(outputStream);
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    if (isLeader) {
      // Only initialize root when isLeader because when initializing root, BlockMaster needs to
      // write journal entry, if it is not leader, BlockMaster won't have a writable journal.
      // If it is standby, it should be able to load the inode tree from leader's checkpoint.
      mInodeTree.initializeRoot(SecurityUtils.getOwnerFromLoginModule(),
          SecurityUtils.getGroupFromLoginModule(), Mode.createFullAccess().applyDirectoryUMask());
      String defaultUFS = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
      try {
        mMountTable.add(new AlluxioURI(MountTable.ROOT), new AlluxioURI(defaultUFS),
            MountOptions.defaults().setShared(
                UnderFileSystemUtils.isObjectStorage(defaultUFS) && Configuration
                    .getBoolean(PropertyKey.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY)));
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        throw new IOException("Failed to mount the default UFS " + defaultUFS);
      }
    }
    // Call super.start after mInodeTree is initialized because mInodeTree is needed to write
    // a journal entry during super.start. Call super.start before calling
    // getExecutorService() because the super.start initializes the executor service.
    super.start(isLeader);
    if (isLeader) {
      mTtlCheckerService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_TTL_CHECK, new MasterInodeTtlCheckExecutor(),
              Configuration.getInt(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS)));
      mLostFilesDetectionService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_LOST_FILES_DETECTION,
              new LostFilesDetectionHeartbeatExecutor(),
              Configuration.getInt(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
      if (Configuration.getBoolean(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)) {
        mStartupConsistencyCheck = getExecutorService().submit(new Callable<List<AlluxioURI>>() {
          @Override
          public List<AlluxioURI> call() throws Exception {
            return startupCheckConsistency(ExecutorServiceFactories
                .fixedThreadPoolExecutorServiceFactory("startup-consistency-check", 32).create());
          }
        });
      }
    }
  }

  /**
   * Checks the consistency of the root in a multi-threaded and incremental fashion. This method
   * will only READ lock the directories and files actively being checked and release them after the
   * check on the file / directory is complete.
   *
   * @return a list of paths in Alluxio which are not consistent with the under storage
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws IOException if an error occurs interacting with the under storage
   */
  private List<AlluxioURI> startupCheckConsistency(final ExecutorService service)
      throws InterruptedException, IOException {
    /** A marker {@link StartupConsistencyChecker}s add to the queue to signal completion */
    final long completionMarker = -1;
    /** A shared queue of directories which have yet to be checked */
    final BlockingQueue<Long> dirsToCheck = new LinkedBlockingQueue<>();

    /**
     * A {@link Callable} which checks the consistency of a directory.
     */
    final class StartupConsistencyChecker implements Callable<List<AlluxioURI>> {
      /** The path to check, guaranteed to be a directory in Alluxio. */
      private final Long mFileId;

      /**
       * Creates a new callable which checks the consistency of a directory.
       * @param fileId the path to check
       */
      private StartupConsistencyChecker(Long fileId) {
        mFileId = fileId;
      }

      /**
       * Checks the consistency of the directory and all immediate children which are files. All
       * immediate children which are directories are added to the shared queue of directories to
       * check. The parent directory is READ locked during the entire call while the children are
       * READ locked only during the consistency check of the children files.
       *
       * @return a list of inconsistent uris
       * @throws IOException if an error occurs interacting with the under storage
       */
      @Override
      public List<AlluxioURI> call() throws IOException {
        List<AlluxioURI> inconsistentUris = new ArrayList<>();
        try (LockedInodePath dir = mInodeTree.lockFullInodePath(mFileId, InodeTree.LockMode.READ)) {
          Inode parentInode = dir.getInode();
          AlluxioURI parentUri = dir.getUri();
          if (!checkConsistencyInternal(parentInode, parentUri)) {
            inconsistentUris.add(parentUri);
          }
          for (Inode childInode : ((InodeDirectory) parentInode).getChildren()) {
            try {
              childInode.lockReadAndCheckParent(parentInode);
            } catch (InvalidPathException e) {
              // This should be safe, continue.
              LOG.debug("Error during startup check consistency, ignoring and continuing.", e);
              continue;
            }
            try {
              AlluxioURI childUri = parentUri.join(childInode.getName());
              if (childInode.isDirectory()) {
                dirsToCheck.add(childInode.getId());
              } else {
                if (!checkConsistencyInternal(childInode, childUri)) {
                  inconsistentUris.add(childUri);
                }
              }
            } finally {
              childInode.unlockRead();
            }
          }
        } catch (FileDoesNotExistException e) {
          // This should be safe, continue.
          LOG.debug("A file scheduled for consistency check was deleted before the check.");
        } catch (InvalidPathException e) {
          // This should not happen.
          LOG.error("An invalid path was discovered during the consistency check, skipping.", e);
        }
        dirsToCheck.add(completionMarker);
        return inconsistentUris;
      }
    }

    // Add the root to the directories to check.
    dirsToCheck.add(mInodeTree.getRoot().getId());
    List<Future<List<AlluxioURI>>> results = new ArrayList<>();
    // Tracks how many checkers have been started.
    long started = 0;
    // Tracks how many checkers have completed.
    long completed = 0;
    do {
      Long fileId = dirsToCheck.take();
      if (fileId == completionMarker) { // A thread signaled completion.
        completed++;
      } else { // A new directory needs to be checked.
        StartupConsistencyChecker checker = new StartupConsistencyChecker(fileId);
        results.add(service.submit(checker));
        started++;
      }
    } while (started != completed);

    // Return the total set of inconsistent paths discovered.
    List<AlluxioURI> inconsistentUris = new ArrayList<>();
    for (Future<List<AlluxioURI>> result : results) {
      try {
        inconsistentUris.addAll(result.get());
      } catch (Exception e) {
        // This shouldn't happen, all futures should be complete.
        Throwables.propagate(e);
      }
    }
    service.shutdown();
    return inconsistentUris;
  }

  /**
   * Class to represent the status and result of the startup consistency check.
   */
  public static final class StartupConsistencyCheck {
    /**
     * Status of the check.
     */
    public enum Status {
      COMPLETE,
      DISABLED,
      FAILED,
      RUNNING
    }

    /**
     * @param inconsistentUris the uris which are inconsistent with the underlying storage
     * @return a result set to the complete status
     */
    public static StartupConsistencyCheck complete(List<AlluxioURI> inconsistentUris) {
      return new StartupConsistencyCheck(Status.COMPLETE, inconsistentUris);
    }

    /**
     * @return a result set to the disabled status
     */
    public static StartupConsistencyCheck disabled() {
      return new StartupConsistencyCheck(Status.DISABLED, null);
    }

    /**
     * @return a result set to the failed status
     */
    public static StartupConsistencyCheck failed() {
      return new StartupConsistencyCheck(Status.FAILED, null);
    }

    /**
     * @return a result set to the running status
     */
    public static StartupConsistencyCheck running() {
      return new StartupConsistencyCheck(Status.RUNNING, null);
    }

    private Status mStatus;
    private List<AlluxioURI> mInconsistentUris;

    /**
     * Create a new startup consistency check result.
     *
     * @param status the state of the check
     * @param inconsistentUris the uris which are inconsistent with the underlying storage
     */
    private StartupConsistencyCheck(Status status, List<AlluxioURI> inconsistentUris) {
      mStatus = status;
      mInconsistentUris = inconsistentUris;
    }

    /**
     * @return the status of the check
     */
    public Status getStatus() {
      return mStatus;
    }

    /**
     * @return the uris which are inconsistent with the underlying storage
     */
    public List<AlluxioURI> getInconsistentUris() {
      return mInconsistentUris;
    }
  }

=======
public interface FileSystemMaster extends Master {
>>>>>>> fae727c2236d0c5dfe53c04006dd70509cb38b94
  /**
   * @return the status of the startup consistency check and inconsistent paths if it is complete
   */
  StartupConsistencyCheck getStartupConsistencyCheck();

  /**
   * Returns the file id for a given path. If the given path does not exist in Alluxio, the method
   * attempts to load it from UFS.
   * <p>
   * This operation requires users to have READ permission of the path.
   *
   * @param path the path to get the file id for
   * @return the file id for a given path, or -1 if there is no file at that path
   * @throws AccessControlException if permission checking fails
   */
  long getFileId(AlluxioURI path) throws AccessControlException;

  /**
   * Returns the {@link FileInfo} for a given file id. This method is not user-facing but supposed
   * to be called by other internal servers (e.g., block workers, lineage master, web UI).
   *
   * @param fileId the file id to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission denied
   */
  // TODO(binfan): Add permission checking for internal APIs
<<<<<<< HEAD
  public FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException {
    Metrics.GET_FILE_INFO_OPS.inc();
    try (
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      // ALLUXIO CS REPLACE
      // return getFileInfoInternal(inodePath);
      // ALLUXIO CS WITH
      FileInfo fileInfo = getFileInfoInternal(inodePath);
      populateCapability(fileInfo, inodePath);
      return fileInfo;
      // ALLUXIO CS END
    }
  }
||||||| merged common ancestors
  public FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException {
    Metrics.GET_FILE_INFO_OPS.inc();
    try (
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      return getFileInfoInternal(inodePath);
    }
  }
=======
  FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException;
>>>>>>> fae727c2236d0c5dfe53c04006dd70509cb38b94

  /**
   * Returns the {@link FileInfo} for a given path.
   * <p>
   * This operation requires users to have READ permission on the path.
   *
   * @param path the path to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path is not valid
   * @throws AccessControlException if permission checking fails
   */
  // TODO(peis): Add an option not to load metadata.
<<<<<<< HEAD
  public FileInfo getFileInfo(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    Metrics.GET_FILE_INFO_OPS.inc();

    // Get a READ lock first to see if we need to load metadata, note that this assumes load
    // metadata for direct children is disabled by default.
    try (LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      if (inodePath.fullPathExists()) {
        // The file already exists, so metadata does not need to be loaded.
        // ALLUXIO CS REPLACE
        // return getFileInfoInternal(inodePath);
        // ALLUXIO CS WITH
        FileInfo fileInfo = getFileInfoInternal(inodePath);
        populateCapability(fileInfo, inodePath);
        return fileInfo;
        // ALLUXIO CS END
      }
    }

    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      // This is WRITE locked, since loading metadata is possible.
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      loadMetadataIfNotExistAndJournal(inodePath,
          LoadMetadataOptions.defaults().setCreateAncestors(true), journalContext);
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
      // ALLUXIO CS REPLACE
      // return getFileInfoInternal(inodePath);
      // ALLUXIO CS WITH
      FileInfo fileInfo = getFileInfoInternal(inodePath);
      populateCapability(fileInfo, inodePath);
      return fileInfo;
      // ALLUXIO CS END
    }
  }

  /**
   * @param inodePath the {@link LockedInodePath} to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given inode
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission denied
   */
  private FileInfo getFileInfoInternal(LockedInodePath inodePath)
      throws FileDoesNotExistException, AccessControlException {
    Inode<?> inode = inodePath.getInode();
    AlluxioURI uri = inodePath.getUri();
    FileInfo fileInfo = inode.generateClientFileInfo(uri.toString());
    fileInfo.setInMemoryPercentage(getInMemoryPercentage(inode));
    if (inode instanceof InodeFile) {
      try {
        fileInfo.setFileBlockInfos(getFileBlockInfoListInternal(inodePath));
      } catch (InvalidPathException e) {
        throw new FileDoesNotExistException(e.getMessage(), e);
      }
    }
    MountTable.Resolution resolution;
    try {
      resolution = mMountTable.resolve(uri);
    } catch (InvalidPathException e) {
      throw new FileDoesNotExistException(e.getMessage(), e);
    }
    AlluxioURI resolvedUri = resolution.getUri();
    // Only set the UFS path if the path is nested under a mount point.
    if (!uri.equals(resolvedUri)) {
      fileInfo.setUfsPath(resolvedUri.toString());
    }
    Metrics.FILE_INFOS_GOT.inc();
    return fileInfo;
  }
||||||| merged common ancestors
  public FileInfo getFileInfo(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    Metrics.GET_FILE_INFO_OPS.inc();

    // Get a READ lock first to see if we need to load metadata, note that this assumes load
    // metadata for direct children is disabled by default.
    try (LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      if (inodePath.fullPathExists()) {
        // The file already exists, so metadata does not need to be loaded.
        return getFileInfoInternal(inodePath);
      }
    }

    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      // This is WRITE locked, since loading metadata is possible.
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      loadMetadataIfNotExistAndJournal(inodePath,
          LoadMetadataOptions.defaults().setCreateAncestors(true), journalContext);
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
      return getFileInfoInternal(inodePath);
    }
  }

  /**
   * @param inodePath the {@link LockedInodePath} to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given inode
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission denied
   */
  private FileInfo getFileInfoInternal(LockedInodePath inodePath)
      throws FileDoesNotExistException, AccessControlException {
    Inode<?> inode = inodePath.getInode();
    AlluxioURI uri = inodePath.getUri();
    FileInfo fileInfo = inode.generateClientFileInfo(uri.toString());
    fileInfo.setInMemoryPercentage(getInMemoryPercentage(inode));
    if (inode instanceof InodeFile) {
      try {
        fileInfo.setFileBlockInfos(getFileBlockInfoListInternal(inodePath));
      } catch (InvalidPathException e) {
        throw new FileDoesNotExistException(e.getMessage(), e);
      }
    }
    MountTable.Resolution resolution;
    try {
      resolution = mMountTable.resolve(uri);
    } catch (InvalidPathException e) {
      throw new FileDoesNotExistException(e.getMessage(), e);
    }
    AlluxioURI resolvedUri = resolution.getUri();
    // Only set the UFS path if the path is nested under a mount point.
    if (!uri.equals(resolvedUri)) {
      fileInfo.setUfsPath(resolvedUri.toString());
    }
    Metrics.FILE_INFOS_GOT.inc();
    return fileInfo;
  }
=======
  FileInfo getFileInfo(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;
>>>>>>> fae727c2236d0c5dfe53c04006dd70509cb38b94

  /**
   * Returns the persistence state for a file id. This method is used by the lineage master.
   *
   * @param fileId the file id
   * @return the {@link PersistenceState} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   */
  // TODO(binfan): Add permission checking for internal APIs
  PersistenceState getPersistenceState(long fileId) throws FileDoesNotExistException;

  /**
   * Returns a list of {@link FileInfo} for a given path. If the given path is a file, the list only
   * contains a single object. If it is a directory, the resulting list contains all direct children
   * of the directory.
   * <p>
   * This operation requires users to have READ permission on the path, and also
   * EXECUTE permission on the path if it is a directory.
   *
   * @param path the path to get the {@link FileInfo} list for
   * @param listStatusOptions the {@link alluxio.master.file.options.ListStatusOptions}
   * @return the list of {@link FileInfo}s
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   */
  List<FileInfo> listStatus(AlluxioURI path, ListStatusOptions listStatusOptions)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException;

  /**
   * @return a read-only view of the file system master
   */
  FileSystemMasterView getFileSystemMasterView();

  /**
   * Checks the consistency of the files and directories in the subtree under the path.
   *
   * @param path the root of the subtree to check
   * @param options the options to use for the checkConsistency method
   * @return a list of paths in Alluxio which are not consistent with the under storage
   * @throws AccessControlException if the permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   * @throws InvalidPathException if the path is invalid
   * @throws IOException if an error occurs interacting with the under storage
   */
  List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyOptions options)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException, IOException;

  /**
   * Completes a file. After a file is completed, it cannot be written to.
   * <p>
   * This operation requires users to have WRITE permission on the path.
   *
   * @param path the file path to complete
   * @param options the method options
   * @throws BlockInfoException if a block information exception is encountered
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws AccessControlException if permission checking fails
   */
  void completeFile(AlluxioURI path, CompleteFileOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, AccessControlException;

  /**
   * Creates a file (not a directory) for a given path.
   * <p>
   * This operation requires WRITE permission on the parent of this path.
   *
   * @param path the file to create
   * @param options method options
   * @return the id of the created file
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if an invalid block information is encountered
   * @throws IOException if the creation fails
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   * option is false
   */
  long createFile(AlluxioURI path, CreateFileOptions options)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
      BlockInfoException, IOException, FileDoesNotExistException;

  /**
   * Reinitializes the blocks of an existing open file.
   *
   * @param path the path to the file
   * @param blockSizeBytes the new block size
   * @param ttl the ttl
   * @param ttlAction action to take after Ttl expiry
   * @return the file id
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the path does not exist
   */
  // Used by lineage master
  long reinitializeFile(AlluxioURI path, long blockSizeBytes, long ttl, TtlAction ttlAction)
      throws InvalidPathException, FileDoesNotExistException;

  /**
   * Gets a new block id for the next block of a given file to write to.
   * <p>
   * This operation requires users to have WRITE permission on the path as
   * this API is called when creating a new block for a file.
   *
   * @param path the path of the file to get the next block id for
   * @return the next block id for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the given path is not valid
   * @throws AccessControlException if permission checking fails
   */
  long getNewBlockIdForFile(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;

  /**
   * @return a copy of the current mount table
   */
  Map<String, MountInfo> getMountTable();

  /**
   * @return the number of files and directories
   */
  int getNumberOfPaths();

  /**
   * @return the number of pinned files and directories
   */
  int getNumberOfPinnedFiles();

  /**
   * Deletes a given path.
   * <p>
   * This operation requires user to have WRITE permission on the parent of the path.
   *
   * @param path the path to delete
   * @param options method options
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void delete(AlluxioURI path, DeleteOptions options) throws IOException,
      FileDoesNotExistException, DirectoryNotEmptyException, InvalidPathException,
      AccessControlException;

  /**
   * Gets the {@link FileBlockInfo} for all blocks of a file. If path is a directory, an exception
   * is thrown.
   * <p>
   * This operation requires the client user to have READ permission on the the path.
   *
   * @param path the path to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given path
   * @throws FileDoesNotExistException if the file does not exist or path is a directory
   * @throws InvalidPathException if the path of the given file is invalid
   * @throws AccessControlException if permission checking fails
   */
  List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;

  /**
   * @return absolute paths of all in memory files
   */
  List<AlluxioURI> getInMemoryFiles();

  /**
   * Creates a directory for a given path.
   * <p>
   * This operation requires the client user to have WRITE permission on the parent of the path.
   *
   * @param path the path of the directory
   * @param options method options
   * @return the id of the created directory
   * @throws InvalidPathException when the path is invalid
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws IOException if a non-Alluxio related exception occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  long createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException;

  /**
   * Renames a file to a destination.
   * <p>
   * This operation requires users to have WRITE permission on the parent of the src path, and
   * WRITE permission on the parent of the dst path.
   *
   * @param srcPath the source path to rename
   * @param dstPath the destination path to rename the file to
   * @param options method options
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileAlreadyExistsException if the file already exists
   */
  void rename(AlluxioURI srcPath, AlluxioURI dstPath, RenameOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException;

  /**
   * Frees or evicts all of the blocks of the file from alluxio storage. If the given file is a
   * directory, and the 'recursive' flag is enabled, all descendant files will also be freed.
   * <p>
   * This operation requires users to have READ permission on the path.
   *
   * @param path the path to free
   * @param options options to free
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   * @throws UnexpectedAlluxioException if the file or directory can not be freed
   */
  // TODO(binfan): throw a better exception rather than UnexpectedAlluxioException. Currently
  // UnexpectedAlluxioException is thrown because we want to keep backwards compatibility with
  // clients of earlier versions prior to 1.5. If a new exception is added, it will be converted
  // into RuntimeException on the client.
  void free(AlluxioURI path, FreeOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException;

  /**
   * Gets the path of a file with the given id.
   *
   * @param fileId the id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  AlluxioURI getPath(long fileId) throws FileDoesNotExistException;

  /**
   * @return the set of inode ids which are pinned
   */
  Set<Long> getPinIdList();

  /**
   * @return the ufs address for this master
   */
  String getUfsAddress();

  /**
   * @return the white list
   */
  List<String> getWhiteList();

  /**
   * @return all the files lost on the workers
   */
  List<Long> getLostFiles();

  /**
   * Reports a file as lost.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  void reportLostFile(long fileId) throws FileDoesNotExistException;

  /**
   * Loads metadata for the object identified by the given path from UFS into Alluxio.
   * <p>
   * This operation requires users to have WRITE permission on the path
   * and its parent path if path is a file, or WRITE permission on the
   * parent path if path is a directory.
   *
   * @param path the path for which metadata should be loaded
   * @param options the load metadata options
   * @return the file id of the loaded path
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws InvalidPathException if invalid path is encountered
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   */
  long loadMetadata(AlluxioURI path, LoadMetadataOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, IOException, AccessControlException;

  /**
   * Mounts a UFS path onto an Alluxio path.
   * <p>
   * This operation requires users to have WRITE permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to mount to
   * @param ufsPath the UFS path to mount
   * @param options the mount options
   * @throws FileAlreadyExistsException if the path to be mounted to already exists
   * @throws FileDoesNotExistException if the parent of the path to be mounted to does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException;

  /**
   * Unmounts a UFS path previously mounted onto an Alluxio path.
   * <p>
   * This operation requires users to have WRITE permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to unmount, must be a mount point
   * @throws FileDoesNotExistException if the path to be mounted does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  void unmount(AlluxioURI alluxioPath)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException;

  /**
   * Resets a file. It first free the whole file, and then reinitializes it.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid for the id of the file
   * @throws UnexpectedAlluxioException if the file or directory can not be freed
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  void resetFile(long fileId)
      throws UnexpectedAlluxioException, FileDoesNotExistException, InvalidPathException,
      AccessControlException;

  /**
   * Sets the file attribute.
   * <p>
   * This operation requires users to have WRITE permission on the path. In
   * addition, the client user must be a super user when setting the owner, and must be a super user
   * or the owner when setting the group or permission.
   *
   * @param path the path to set attribute for
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   */
<<<<<<< HEAD
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    Metrics.SET_ATTRIBUTE_OPS.inc();
    // for chown
    boolean rootRequired = options.getOwner() != null;
    // for chgrp, chmod
    boolean ownerRequired =
        (options.getGroup() != null) || (options.getMode() != Constants.INVALID_MODE);
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkSetAttributePermission(inodePath, rootRequired, ownerRequired);
      setAttributeAndJournal(inodePath, rootRequired, ownerRequired, options, journalContext);
    }
  }

  /**
   * Sets the file attribute.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link LockedInodePath} to set attribute for
   * @param rootRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @param journalContext the journal context
   * @throws InvalidPathException if the given path is invalid
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   */
  private void setAttributeAndJournal(LockedInodePath inodePath, boolean rootRequired,
      boolean ownerRequired, SetAttributeOptions options, JournalContext journalContext)
      throws InvalidPathException, FileDoesNotExistException, AccessControlException {
    Inode<?> targetInode = inodePath.getInode();
    long opTimeMs = System.currentTimeMillis();
    if (options.isRecursive() && targetInode.isDirectory()) {
      try (InodeLockList lockList = mInodeTree
          .lockDescendants(inodePath, InodeTree.LockMode.WRITE)) {
        List<Inode<?>> inodeChildren = lockList.getInodes();
        for (Inode<?> inode : inodeChildren) {
          // the path to inode for getPath should already be locked.
          try (LockedInodePath childPath = mInodeTree
              .lockFullInodePath(mInodeTree.getPath(inode), InodeTree.LockMode.READ)) {
            // TODO(gpang): a better way to check permissions
            mPermissionChecker.checkSetAttributePermission(childPath, rootRequired, ownerRequired);
          }
        }
        TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(inodePath);
        for (Inode<?> inode : inodeChildren) {
          // the path to inode for getPath should already be locked.
          tempInodePath.setDescendant(inode, mInodeTree.getPath(inode));
          List<Inode<?>> persistedInodes =
              setAttributeInternal(tempInodePath, false, opTimeMs, options);
          journalPersistedInodes(persistedInodes, journalContext);
          journalSetAttribute(tempInodePath, opTimeMs, options, journalContext);
        }
      }
    }
    List<Inode<?>> persistedInodes = setAttributeInternal(inodePath, false, opTimeMs, options);
    journalPersistedInodes(persistedInodes, journalContext);
    journalSetAttribute(inodePath, opTimeMs, options, journalContext);
  }

  /**
   * @param inodePath the file path to use
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   * @param journalContext the journal context
   * @throws FileDoesNotExistException if path does not exist
   */
  private void journalSetAttribute(LockedInodePath inodePath, long opTimeMs,
      SetAttributeOptions options, JournalContext journalContext) throws FileDoesNotExistException {
    SetAttributeEntry.Builder builder =
        SetAttributeEntry.newBuilder().setId(inodePath.getInode().getId()).setOpTimeMs(opTimeMs);
    if (options.getPinned() != null) {
      builder.setPinned(options.getPinned());
    }
    if (options.getTtl() != null) {
      builder.setTtl(options.getTtl());
      builder.setTtlAction(ProtobufUtils.toProtobuf(options.getTtlAction()));
    }

    // ALLUXIO CS ADD
    if (options.getReplicationMax() != null) {
      builder.setReplicationMax(options.getReplicationMax());
    }
    if (options.getReplicationMin() != null) {
      builder.setReplicationMin(options.getReplicationMin());
    }
    // ALLUXIO CS END
    if (options.getPersisted() != null) {
      builder.setPersisted(options.getPersisted());
    }
    if (options.getOwner() != null) {
      builder.setOwner(options.getOwner());
    }
    if (options.getGroup() != null) {
      builder.setGroup(options.getGroup());
    }
    if (options.getMode() != Constants.INVALID_MODE) {
      builder.setPermission(options.getMode());
    }
    appendJournalEntry(JournalEntry.newBuilder().setSetAttribute(builder).build(), journalContext);
  }
||||||| merged common ancestors
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    Metrics.SET_ATTRIBUTE_OPS.inc();
    // for chown
    boolean rootRequired = options.getOwner() != null;
    // for chgrp, chmod
    boolean ownerRequired =
        (options.getGroup() != null) || (options.getMode() != Constants.INVALID_MODE);
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkSetAttributePermission(inodePath, rootRequired, ownerRequired);
      setAttributeAndJournal(inodePath, rootRequired, ownerRequired, options, journalContext);
    }
  }

  /**
   * Sets the file attribute.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link LockedInodePath} to set attribute for
   * @param rootRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @param journalContext the journal context
   * @throws InvalidPathException if the given path is invalid
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   */
  private void setAttributeAndJournal(LockedInodePath inodePath, boolean rootRequired,
      boolean ownerRequired, SetAttributeOptions options, JournalContext journalContext)
      throws InvalidPathException, FileDoesNotExistException, AccessControlException {
    Inode<?> targetInode = inodePath.getInode();
    long opTimeMs = System.currentTimeMillis();
    if (options.isRecursive() && targetInode.isDirectory()) {
      try (InodeLockList lockList = mInodeTree
          .lockDescendants(inodePath, InodeTree.LockMode.WRITE)) {
        List<Inode<?>> inodeChildren = lockList.getInodes();
        for (Inode<?> inode : inodeChildren) {
          // the path to inode for getPath should already be locked.
          try (LockedInodePath childPath = mInodeTree
              .lockFullInodePath(mInodeTree.getPath(inode), InodeTree.LockMode.READ)) {
            // TODO(gpang): a better way to check permissions
            mPermissionChecker.checkSetAttributePermission(childPath, rootRequired, ownerRequired);
          }
        }
        TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(inodePath);
        for (Inode<?> inode : inodeChildren) {
          // the path to inode for getPath should already be locked.
          tempInodePath.setDescendant(inode, mInodeTree.getPath(inode));
          List<Inode<?>> persistedInodes =
              setAttributeInternal(tempInodePath, false, opTimeMs, options);
          journalPersistedInodes(persistedInodes, journalContext);
          journalSetAttribute(tempInodePath, opTimeMs, options, journalContext);
        }
      }
    }
    List<Inode<?>> persistedInodes = setAttributeInternal(inodePath, false, opTimeMs, options);
    journalPersistedInodes(persistedInodes, journalContext);
    journalSetAttribute(inodePath, opTimeMs, options, journalContext);
  }

  /**
   * @param inodePath the file path to use
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   * @param journalContext the journal context
   * @throws FileDoesNotExistException if path does not exist
   */
  private void journalSetAttribute(LockedInodePath inodePath, long opTimeMs,
      SetAttributeOptions options, JournalContext journalContext) throws FileDoesNotExistException {
    SetAttributeEntry.Builder builder =
        SetAttributeEntry.newBuilder().setId(inodePath.getInode().getId()).setOpTimeMs(opTimeMs);
    if (options.getPinned() != null) {
      builder.setPinned(options.getPinned());
    }
    if (options.getTtl() != null) {
      builder.setTtl(options.getTtl());
      builder.setTtlAction(ProtobufUtils.toProtobuf(options.getTtlAction()));
    }

    if (options.getPersisted() != null) {
      builder.setPersisted(options.getPersisted());
    }
    if (options.getOwner() != null) {
      builder.setOwner(options.getOwner());
    }
    if (options.getGroup() != null) {
      builder.setGroup(options.getGroup());
    }
    if (options.getMode() != Constants.INVALID_MODE) {
      builder.setPermission(options.getMode());
    }
    appendJournalEntry(JournalEntry.newBuilder().setSetAttribute(builder).build(), journalContext);
  }
=======
  void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException;
>>>>>>> fae727c2236d0c5dfe53c04006dd70509cb38b94

  /**
   * Schedules a file for async persistence.
   *
   * @param path the path of the file for persistence
   * @throws AlluxioException if scheduling fails
   */
<<<<<<< HEAD
  public void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException {
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      scheduleAsyncPersistenceAndJournal(inodePath, journalContext);
    }
    // ALLUXIO CS REMOVE
    // // NOTE: persistence is asynchronous so there is no guarantee the path will still exist
    // mAsyncPersistHandler.scheduleAsyncPersistence(path);
    // ALLUXIO CS END
  }

  /**
   * Schedules a file for async persistence.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link LockedInodePath} of the file for persistence
   * @param journalContext the journal context
   * @throws AlluxioException if scheduling fails
   */
  private void scheduleAsyncPersistenceAndJournal(LockedInodePath inodePath,
      JournalContext journalContext) throws AlluxioException {
    long fileId = inodePath.getInode().getId();
    scheduleAsyncPersistenceInternal(inodePath);
    // write to journal
    AsyncPersistRequestEntry asyncPersistRequestEntry =
        AsyncPersistRequestEntry.newBuilder().setFileId(fileId).build();
    appendJournalEntry(
        JournalEntry.newBuilder().setAsyncPersistRequest(asyncPersistRequestEntry).build(),
        journalContext);
  }

  /**
   * @param inodePath the {@link LockedInodePath} of the file to persist
   * @throws AlluxioException if scheduling fails
   */
  private void scheduleAsyncPersistenceInternal(LockedInodePath inodePath) throws AlluxioException {
    inodePath.getInode().setPersistenceState(PersistenceState.TO_BE_PERSISTED);
    // ALLUXIO CS ADD
    long fileId = inodePath.getInode().getId();
    mPersistRequests.put(fileId, new PersistRequest(fileId));
    // ALLUXIO CS END
  }
||||||| merged common ancestors
  public void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException {
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      scheduleAsyncPersistenceAndJournal(inodePath, journalContext);
    }
    // NOTE: persistence is asynchronous so there is no guarantee the path will still exist
    mAsyncPersistHandler.scheduleAsyncPersistence(path);
  }

  /**
   * Schedules a file for async persistence.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link LockedInodePath} of the file for persistence
   * @param journalContext the journal context
   * @throws AlluxioException if scheduling fails
   */
  private void scheduleAsyncPersistenceAndJournal(LockedInodePath inodePath,
      JournalContext journalContext) throws AlluxioException {
    long fileId = inodePath.getInode().getId();
    scheduleAsyncPersistenceInternal(inodePath);
    // write to journal
    AsyncPersistRequestEntry asyncPersistRequestEntry =
        AsyncPersistRequestEntry.newBuilder().setFileId(fileId).build();
    appendJournalEntry(
        JournalEntry.newBuilder().setAsyncPersistRequest(asyncPersistRequestEntry).build(),
        journalContext);
  }

  /**
   * @param inodePath the {@link LockedInodePath} of the file to persist
   * @throws AlluxioException if scheduling fails
   */
  private void scheduleAsyncPersistenceInternal(LockedInodePath inodePath) throws AlluxioException {
    inodePath.getInode().setPersistenceState(PersistenceState.TO_BE_PERSISTED);
  }
=======
  void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException;
>>>>>>> fae727c2236d0c5dfe53c04006dd70509cb38b94

  /**
   * Instructs a worker to persist the files.
   * <p>
   * Needs WRITE permission on the list of files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files that persisted on the worker
   * @return the command for persisting the blocks of a file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if permission checking fails
   */
<<<<<<< HEAD
  public FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    for (long fileId : persistedFiles) {
      try {
        // Permission checking for each file is performed inside setAttribute
        setAttribute(getPath(fileId), SetAttributeOptions.defaults().setPersisted(true));
      } catch (FileDoesNotExistException | AccessControlException | InvalidPathException e) {
        LOG.error("Failed to set file {} as persisted, because {}", fileId, e);
      }
    }

    // get the files for the given worker to persist
    // ALLUXIO CS REPLACE
    // List<PersistFile> filesToPersist = mAsyncPersistHandler.pollFilesToPersist(workerId);
    // if (!filesToPersist.isEmpty()) {
    //   LOG.debug("Sent files {} to worker {} to persist", filesToPersist, workerId);
    // }
    // ALLUXIO CS WITH
    // Worker should not persist any files. Instead, files are persisted through job service.
    List<PersistFile> filesToPersist = new ArrayList<>();
    // ALLUXIO CS END
    FileSystemCommandOptions options = new FileSystemCommandOptions();
    options.setPersistOptions(new PersistCommandOptions(filesToPersist));
    return new FileSystemCommand(CommandType.Persist, options);
  }

  /**
   * @param inodePath the {@link LockedInodePath} to use
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   * @return list of inodes which were marked as persisted
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if failed to set permission
   */
  private List<Inode<?>> setAttributeInternal(LockedInodePath inodePath, boolean replayed,
      long opTimeMs, SetAttributeOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    List<Inode<?>> persistedInodes = Collections.emptyList();
    Inode<?> inode = inodePath.getInode();
    if (options.getPinned() != null) {
      mInodeTree.setPinned(inodePath, options.getPinned(), opTimeMs);
      inode.setLastModificationTimeMs(opTimeMs);
    }
    // ALLUXIO CS ADD
    if (options.getReplicationMax() != null || options.getReplicationMin() != null) {
      Integer replicationMax = options.getReplicationMax();
      Integer replicationMin = options.getReplicationMin();
      mInodeTree.setReplication(inodePath, replicationMax, replicationMin, opTimeMs);
      inode.setLastModificationTimeMs(opTimeMs);
    }
    if (options.getPersistJobId() != null || options.getTempUfsPath() != null) {
      InodeFile file = (InodeFile) inode;
      file.setPersistJobId(options.getPersistJobId());
      file.setTempUfsPath(options.getTempUfsPath());
      if (replayed && options.getPersistJobId() != -1 && !options.getTempUfsPath().isEmpty()) {
        long fileId = file.getId();
        mPersistRequests.remove(fileId);
        mPersistJobs.put(fileId,
            new PersistJob(fileId, options.getPersistJobId(), options.getTempUfsPath()));
      }
      inode.setLastModificationTimeMs(opTimeMs);
    }
    // ALLUXIO CS END
    if (options.getTtl() != null) {
      long ttl = options.getTtl();
      if (inode.getTtl() != ttl) {
        mTtlBuckets.remove(inode);
        inode.setTtl(ttl);
        mTtlBuckets.insert(inode);
        inode.setLastModificationTimeMs(opTimeMs);
        inode.setTtlAction(options.getTtlAction());
      }
    }
    if (options.getPersisted() != null) {
      Preconditions.checkArgument(inode.isFile(), PreconditionMessage.PERSIST_ONLY_FOR_FILE);
      Preconditions.checkArgument(((InodeFile) inode).isCompleted(),
          PreconditionMessage.FILE_TO_PERSIST_MUST_BE_COMPLETE);
      InodeFile file = (InodeFile) inode;
      // TODO(manugoyal) figure out valid behavior in the un-persist case
      Preconditions
          .checkArgument(options.getPersisted(), PreconditionMessage.ERR_SET_STATE_UNPERSIST);
      if (!file.isPersisted()) {
        file.setPersistenceState(PersistenceState.PERSISTED);
        persistedInodes = propagatePersistedInternal(inodePath, false);
        file.setLastModificationTimeMs(opTimeMs);
        Metrics.FILES_PERSISTED.inc();
      }
    }
    boolean ownerGroupChanged = (options.getOwner() != null) || (options.getGroup() != null);
    boolean modeChanged = (options.getMode() != Constants.INVALID_MODE);
    // If the file is persisted in UFS, also update corresponding owner/group/permission.
    if ((ownerGroupChanged || modeChanged) && !replayed && inode.isPersisted()) {
      if ((inode instanceof InodeFile) && !((InodeFile) inode).isCompleted()) {
        LOG.debug("Alluxio does not propagate chown/chgrp/chmod to UFS for incomplete files.");
      } else {
        MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
        String ufsUri = resolution.getUri().toString();
        if (UnderFileSystemUtils.isObjectStorage(ufsUri)) {
          LOG.warn("setOwner/setMode is not supported to object storage UFS via Alluxio. " + "UFS: "
              + ufsUri + ". This has no effect on the underlying object.");
        } else {
          UnderFileSystem ufs = resolution.getUfs();
          if (ownerGroupChanged) {
            try {
              String owner = options.getOwner() != null ? options.getOwner() : inode.getOwner();
              String group = options.getGroup() != null ? options.getGroup() : inode.getGroup();
              ufs.setOwner(ufsUri, owner, group);
            } catch (IOException e) {
              throw new AccessControlException("Could not setOwner for UFS file " + ufsUri
                  + " . Aborting the setAttribute operation in Alluxio.", e);
            }
          }
          if (modeChanged) {
            try {
              ufs.setMode(ufsUri, options.getMode());
            } catch (IOException e) {
              throw new AccessControlException("Could not setMode for UFS file " + ufsUri
                  + " . Aborting the setAttribute operation in Alluxio.", e);
            }
          }
        }
      }
    }
    // Only commit the set permission to inode after the propagation to UFS succeeded.
    if (options.getOwner() != null) {
      inode.setOwner(options.getOwner());
    }
    if (options.getGroup() != null) {
      inode.setGroup(options.getGroup());
    }
    if (modeChanged) {
      inode.setMode(options.getMode());
    }
    return persistedInodes;
  }
  // ALLUXIO CS ADD

  /**
   * Populates the {@link alluxio.security.capability.Capability} for a file.
   *
   * @param fileInfo the fileInfo of the file
   * @param inodePath the inode path of the file
   * @throws AccessControlException if permission denied
   */
  private void populateCapability(FileInfo fileInfo, LockedInodePath inodePath)
      throws AccessControlException {
    if (mBlockMaster.getCapabilityEnabled()) {
      alluxio.proto.security.CapabilityProto.Content content =
          alluxio.proto.security.CapabilityProto.Content.newBuilder()
              .setAccessMode(mPermissionChecker.getPermission(inodePath).ordinal())
              .setUser(alluxio.security.authentication.AuthenticatedClientUser.getClientUser())
              .setExpirationTimeMs(
                  alluxio.util.CommonUtils.getCurrentMs() + mBlockMaster.getCapabilityLifeTimeMs())
              .setFileId(fileInfo.getFileId()).build();
      fileInfo.setCapability(
          new alluxio.security.capability.Capability(
              mBlockMaster.getCapabilityKeyManager().getCapabilityKey(), content));
    }
  }

  // ALLUXIO CS END

  /**
   * @param entry the entry to use
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if failed to set permission
   */
  private void setAttributeFromEntry(SetAttributeEntry entry)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    if (entry.hasPinned()) {
      options.setPinned(entry.getPinned());
    }
    if (entry.hasTtl()) {
      options.setTtl(entry.getTtl());
      options.setTtlAction(ProtobufUtils.fromProtobuf(entry.getTtlAction()));
    }
    if (entry.hasPersisted()) {
      options.setPersisted(entry.getPersisted());
    }
    if (entry.hasOwner()) {
      options.setOwner(entry.getOwner());
    }
    if (entry.hasGroup()) {
      options.setGroup(entry.getGroup());
    }
    if (entry.hasPermission()) {
      options.setMode((short) entry.getPermission());
    }
    // ALLUXIO CS ADD
    if (entry.hasPersistJobId()) {
      options.setPersistJobId(entry.getPersistJobId());
    }
    if (entry.hasReplicationMax()) {
      options.setReplicationMax(entry.getReplicationMax());
    }
    if (entry.hasReplicationMin()) {
      options.setReplicationMin(entry.getReplicationMin());
    }
    if (entry.hasTempUfsPath()) {
      options.setTempUfsPath(entry.getTempUfsPath());
    }
    // ALLUXIO CS END
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      setAttributeInternal(inodePath, true, entry.getOpTimeMs(), options);
      // Intentionally not journaling the persisted inodes from setAttributeInternal
    }
  }
||||||| merged common ancestors
  public FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    for (long fileId : persistedFiles) {
      try {
        // Permission checking for each file is performed inside setAttribute
        setAttribute(getPath(fileId), SetAttributeOptions.defaults().setPersisted(true));
      } catch (FileDoesNotExistException | AccessControlException | InvalidPathException e) {
        LOG.error("Failed to set file {} as persisted, because {}", fileId, e);
      }
    }

    // get the files for the given worker to persist
    List<PersistFile> filesToPersist = mAsyncPersistHandler.pollFilesToPersist(workerId);
    if (!filesToPersist.isEmpty()) {
      LOG.debug("Sent files {} to worker {} to persist", filesToPersist, workerId);
    }
    FileSystemCommandOptions options = new FileSystemCommandOptions();
    options.setPersistOptions(new PersistCommandOptions(filesToPersist));
    return new FileSystemCommand(CommandType.Persist, options);
  }

  /**
   * @param inodePath the {@link LockedInodePath} to use
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   * @return list of inodes which were marked as persisted
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if failed to set permission
   */
  private List<Inode<?>> setAttributeInternal(LockedInodePath inodePath, boolean replayed,
      long opTimeMs, SetAttributeOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    List<Inode<?>> persistedInodes = Collections.emptyList();
    Inode<?> inode = inodePath.getInode();
    if (options.getPinned() != null) {
      mInodeTree.setPinned(inodePath, options.getPinned(), opTimeMs);
      inode.setLastModificationTimeMs(opTimeMs);
    }
    if (options.getTtl() != null) {
      long ttl = options.getTtl();
      if (inode.getTtl() != ttl) {
        mTtlBuckets.remove(inode);
        inode.setTtl(ttl);
        mTtlBuckets.insert(inode);
        inode.setLastModificationTimeMs(opTimeMs);
        inode.setTtlAction(options.getTtlAction());
      }
    }
    if (options.getPersisted() != null) {
      Preconditions.checkArgument(inode.isFile(), PreconditionMessage.PERSIST_ONLY_FOR_FILE);
      Preconditions.checkArgument(((InodeFile) inode).isCompleted(),
          PreconditionMessage.FILE_TO_PERSIST_MUST_BE_COMPLETE);
      InodeFile file = (InodeFile) inode;
      // TODO(manugoyal) figure out valid behavior in the un-persist case
      Preconditions
          .checkArgument(options.getPersisted(), PreconditionMessage.ERR_SET_STATE_UNPERSIST);
      if (!file.isPersisted()) {
        file.setPersistenceState(PersistenceState.PERSISTED);
        persistedInodes = propagatePersistedInternal(inodePath, false);
        file.setLastModificationTimeMs(opTimeMs);
        Metrics.FILES_PERSISTED.inc();
      }
    }
    boolean ownerGroupChanged = (options.getOwner() != null) || (options.getGroup() != null);
    boolean modeChanged = (options.getMode() != Constants.INVALID_MODE);
    // If the file is persisted in UFS, also update corresponding owner/group/permission.
    if ((ownerGroupChanged || modeChanged) && !replayed && inode.isPersisted()) {
      if ((inode instanceof InodeFile) && !((InodeFile) inode).isCompleted()) {
        LOG.debug("Alluxio does not propagate chown/chgrp/chmod to UFS for incomplete files.");
      } else {
        MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
        String ufsUri = resolution.getUri().toString();
        if (UnderFileSystemUtils.isObjectStorage(ufsUri)) {
          LOG.warn("setOwner/setMode is not supported to object storage UFS via Alluxio. " + "UFS: "
              + ufsUri + ". This has no effect on the underlying object.");
        } else {
          UnderFileSystem ufs = resolution.getUfs();
          if (ownerGroupChanged) {
            try {
              String owner = options.getOwner() != null ? options.getOwner() : inode.getOwner();
              String group = options.getGroup() != null ? options.getGroup() : inode.getGroup();
              ufs.setOwner(ufsUri, owner, group);
            } catch (IOException e) {
              throw new AccessControlException("Could not setOwner for UFS file " + ufsUri
                  + " . Aborting the setAttribute operation in Alluxio.", e);
            }
          }
          if (modeChanged) {
            try {
              ufs.setMode(ufsUri, options.getMode());
            } catch (IOException e) {
              throw new AccessControlException("Could not setMode for UFS file " + ufsUri
                  + " . Aborting the setAttribute operation in Alluxio.", e);
            }
          }
        }
      }
    }
    // Only commit the set permission to inode after the propagation to UFS succeeded.
    if (options.getOwner() != null) {
      inode.setOwner(options.getOwner());
    }
    if (options.getGroup() != null) {
      inode.setGroup(options.getGroup());
    }
    if (modeChanged) {
      inode.setMode(options.getMode());
    }
    return persistedInodes;
  }

  /**
   * @param entry the entry to use
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if failed to set permission
   */
  private void setAttributeFromEntry(SetAttributeEntry entry)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    if (entry.hasPinned()) {
      options.setPinned(entry.getPinned());
    }
    if (entry.hasTtl()) {
      options.setTtl(entry.getTtl());
      options.setTtlAction(ProtobufUtils.fromProtobuf(entry.getTtlAction()));
    }
    if (entry.hasPersisted()) {
      options.setPersisted(entry.getPersisted());
    }
    if (entry.hasOwner()) {
      options.setOwner(entry.getOwner());
    }
    if (entry.hasGroup()) {
      options.setGroup(entry.getGroup());
    }
    if (entry.hasPermission()) {
      options.setMode((short) entry.getPermission());
    }
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      setAttributeInternal(inodePath, true, entry.getOpTimeMs(), options);
      // Intentionally not journaling the persisted inodes from setAttributeInternal
    }
  }
=======
  FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;
>>>>>>> fae727c2236d0c5dfe53c04006dd70509cb38b94

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
<<<<<<< HEAD
  public List<WorkerInfo> getWorkerInfoList() {
    return mBlockMaster.getWorkerInfoList();
  }

  /**
   * This class represents the executor for periodic inode ttl check.
   */
  private final class MasterInodeTtlCheckExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link MasterInodeTtlCheckExecutor}.
     */
    public MasterInodeTtlCheckExecutor() {}

    @Override
    public void heartbeat() {
      Set<TtlBucket> expiredBuckets = mTtlBuckets.getExpiredBuckets(System.currentTimeMillis());
      for (TtlBucket bucket : expiredBuckets) {
        for (Inode inode : bucket.getInodes()) {
          AlluxioURI path = null;
          try (LockedInodePath inodePath = mInodeTree
              .lockFullInodePath(inode.getId(), InodeTree.LockMode.READ)) {
            path = inodePath.getUri();
          } catch (Exception e) {
            LOG.error("Exception trying to clean up {} for ttl check: {}", inode.toString(),
                e.toString());
          }
          if (path != null) {
            try {
              TtlAction ttlAction = inode.getTtlAction();
              LOG.debug("Path {} TTL has expired, performing action {}", path.getPath(), ttlAction);
              switch (ttlAction) {
                case FREE:
                  // public free method will lock the path, and check WRITE permission required at
                  // parent of file
                  if (inode.isDirectory()) {
                    free(path, FreeOptions.defaults().setForced(true).setRecursive(true));
                  } else {
                    free(path, FreeOptions.defaults().setForced(true));
                  }
                  // Reset state
                  inode.setTtl(Constants.NO_TTL);
                  inode.setTtlAction(TtlAction.DELETE);
                  mTtlBuckets.remove(inode);
                  break;
                case DELETE:// Default if not set is DELETE
                  // public delete method will lock the path, and check WRITE permission required at
                  // parent of file
                  if (inode.isDirectory()) {
                    delete(path, DeleteOptions.defaults().setRecursive(true));
                  } else {
                    delete(path, DeleteOptions.defaults().setRecursive(false));
                  }
                  break;
                default:
                  LOG.error("Unknown ttl action {}", ttlAction);
              }
            } catch (Exception e) {
              LOG.error("Exception trying to clean up {} for ttl check: {}", inode.toString(),
                  e.toString());
            }
          }
        }
      }
      mTtlBuckets.removeBuckets(expiredBuckets);
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Lost files periodic check.
   */
  private final class LostFilesDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostFilesDetectionHeartbeatExecutor}.
     */
    public LostFilesDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat() {
      for (long fileId : getLostFiles()) {
        // update the state
        try (LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
          Inode<?> inode = inodePath.getInode();
          if (inode.getPersistenceState() != PersistenceState.PERSISTED) {
            inode.setPersistenceState(PersistenceState.LOST);
          }
        } catch (FileDoesNotExistException e) {
          LOG.error("Exception trying to get inode from inode tree: {}", e.toString());
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  // ALLUXIO CS ADD

  /**
   * Periodically schedules jobs to persist files and updates metadata accordingly.
   */
  @NotThreadSafe
  private final class PersistenceScheduler implements HeartbeatExecutor {

    /**
     * Creates a new instance of {@link PersistenceScheduler}.
     */
    PersistenceScheduler() {}

    @Override
    public void close() {} // Nothing to clean up

    @Override
    public void heartbeat() throws InterruptedException {
      // Process persist requests.
      for (long fileId : mPersistRequests.keySet()) {
        PersistRequest request = mPersistRequests.get(fileId);
        AlluxioURI uri;
        String tempUfsPath;
        try (JournalContext journalContext = createJournalContext()) {
          // If maximum number of attempts have been reached, give up.
          if (!request.getRetryPolicy().attemptRetry()) {
            LOG.warn("Failed to persist file (id={}) in {} attempts.", fileId,
                request.getRetryPolicy().getRetryCount());
            continue;
          }

          // Lookup relevant file information.
          try (LockedInodePath inodePath = mInodeTree
              .lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
            uri = inodePath.getUri();
            InodeFile inode = inodePath.getInodeFile();
            switch (inode.getPersistenceState()) {
              case PERSISTED:
                continue;
              default:
                tempUfsPath = inode.getTempUfsPath();
            }
          }

          // If previous persist job failed, clean up the temporary file.
          cleanup(tempUfsPath);

          // Generate a temporary path to be used by the persist job.
          MountTable.Resolution resolution = mMountTable.resolve(uri);
          tempUfsPath = PathUtils
              .temporaryFileName(System.currentTimeMillis(), resolution.getUri().toString());
          alluxio.job.persist.PersistConfig config =
              new alluxio.job.persist.PersistConfig(uri.getPath(), tempUfsPath, false);

          // Schedule the persist job.
          long jobId = alluxio.client.job.JobThriftClientUtils.start(config);
          mPersistJobs.put(fileId,
              new PersistJob(fileId, jobId, tempUfsPath).setRetryPolicy(request.getRetryPolicy()));

          // Update the inode and journal the change.
          try (LockedInodePath inodePath = mInodeTree
              .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
            InodeFile inode = inodePath.getInodeFile();
            inode.setPersistJobId(jobId);
            inode.setTempUfsPath(tempUfsPath);
            SetAttributeEntry.Builder builder =
                SetAttributeEntry.newBuilder().setId(inode.getId()).setPersistJobId(jobId)
                    .setTempUfsPath(tempUfsPath);
            appendJournalEntry(JournalEntry.newBuilder().setSetAttribute(builder).build(),
                journalContext);
          }
        } catch (FileDoesNotExistException | InvalidPathException e) {
          LOG.warn("The file to be persisted (id={}) no longer exists.", fileId, e);
        } catch (Exception e) {
          LOG.warn("Unexpected exception encountered when starting a job to persist file (id={}).",
              fileId, e);
        } finally {
          mPersistRequests.remove(fileId);
        }
      }
    }
  }

  /**
   * Periodically polls for the result of the jobs and updates metadata accordingly.
   */
  @NotThreadSafe
  private final class PersistenceChecker implements HeartbeatExecutor {

    /**
     * Creates a new instance of {@link PersistenceChecker}.
     */
    PersistenceChecker() {}

    @Override
    public void close() {} // nothing to clean up

    private void handleCompletion(PersistJob job) {
      long fileId = job.getFileId();
      String tempUfsPath = job.getTempUfsPath();
      try (JournalContext journalContext = createJournalContext();
          LockedInodePath inodePath = mInodeTree
              .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
        InodeFile inode = inodePath.getInodeFile();
        switch (inode.getPersistenceState()) {
          case PERSISTED:
            // This can happen if multiple persist requests are issued (e.g. through CLI).
            LOG.warn("File {} has already been persisted.", inodePath.getUri().toString());
            break;
          case TO_BE_PERSISTED:
            MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
            UnderFileSystem ufs = resolution.getUfs();
            String ufsPath = resolution.getUri().toString();
            if (!ufs.renameFile(tempUfsPath, ufsPath)) {
              throw new IOException(
                  String.format("Failed to rename %s to %s.", tempUfsPath, ufsPath));
            }
            ufs.setOwner(ufsPath, inode.getOwner(), inode.getGroup());
            ufs.setMode(ufsPath, inode.getMode());

            inode.setPersistenceState(PersistenceState.PERSISTED);
            inode.setPersistJobId(Constants.PERSISTENCE_INVALID_JOB_ID);
            inode.setTempUfsPath(Constants.PERSISTENCE_INVALID_UFS_PATH);
            journalPersistedInodes(propagatePersistedInternal(inodePath, false), journalContext);

            // Journal the action.
            SetAttributeEntry.Builder builder =
                SetAttributeEntry.newBuilder().setId(inode.getId()).setPersisted(true)
                    .setPersistJobId(Constants.PERSISTENCE_INVALID_JOB_ID)
                    .setTempUfsPath(Constants.PERSISTENCE_INVALID_UFS_PATH);
            appendJournalEntry(JournalEntry.newBuilder().setSetAttribute(builder).build(),
                journalContext);
            break;
          default:
            LOG.error("Unexpected persistence state {}.", inode.getPersistenceState());
        }
      } catch (FileDoesNotExistException | InvalidPathException e) {
        LOG.warn("The file to be persisted (id={}) no longer exists.", fileId);
        // Cleanup the temporary file.
        cleanup(tempUfsPath);
      } catch (IOException e) {
        mPersistRequests.put(fileId, new PersistRequest(fileId).setRetryPolicy(job.getRetryPolicy()));
      } finally {
        mPersistJobs.remove(fileId);
      }
    }

    @Override
    public void heartbeat() throws InterruptedException {
      // Check the progress of persist jobs.
      for (long fileId : mPersistJobs.keySet()) {
        PersistJob job = mPersistJobs.get(fileId);
        long jobId = job.getJobId();

        try {
          alluxio.job.wire.JobInfo jobInfo =
              alluxio.client.job.JobThriftClientUtils.getStatus(jobId);
          switch (jobInfo.getStatus()) {
            case RUNNING:
              // fall through
            case CREATED:
              break;
            case FAILED:
              mPersistJobs.remove(fileId);
              mPersistRequests
                  .put(fileId, new PersistRequest(fileId).setRetryPolicy(job.getRetryPolicy()));
              break;
            case CANCELED:
              mPersistJobs.remove(fileId);
              break;
            case COMPLETED:
              handleCompletion(job);
              break;
            default:
              throw new IllegalStateException("Unrecognized job status: " + jobInfo.getStatus());
          }
        } catch (Exception e) {
          LOG.warn(
              "Unexpected exception encountered when trying to retrieve the status of a job to "
                  + "persist file (id={}).", fileId, e);
          mPersistJobs.remove(fileId);
          mPersistRequests
              .put(fileId, new PersistRequest(fileId).setRetryPolicy(job.getRetryPolicy()));
        }
      }
    }
  }

  private static void cleanup(String ufsPath) {
    final String errMessage = "Failed to delete UFS file {}.";
    if (!ufsPath.isEmpty()) {
      try {
        UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
        if (!ufs.deleteFile(ufsPath)) {
          LOG.warn(errMessage, ufsPath);
        }
      } catch (IOException e) {
        LOG.warn(errMessage, ufsPath, e);
      }
    }
  }
  // ALLUXIO CS END
  /**
   * Class that contains metrics for FileSystemMaster.
   * This class is public because the counter names are referenced in
   * {@link alluxio.web.WebInterfaceMasterMetricsServlet}.
   */
  public static final class Metrics {
    private static final Counter DIRECTORIES_CREATED =
        MetricsSystem.masterCounter("DirectoriesCreated");
    private static final Counter FILE_BLOCK_INFOS_GOT =
        MetricsSystem.masterCounter("FileBlockInfosGot");
    private static final Counter FILE_INFOS_GOT = MetricsSystem.masterCounter("FileInfosGot");
    private static final Counter FILES_COMPLETED = MetricsSystem.masterCounter("FilesCompleted");
    private static final Counter FILES_CREATED = MetricsSystem.masterCounter("FilesCreated");
    private static final Counter FILES_FREED = MetricsSystem.masterCounter("FilesFreed");
    private static final Counter FILES_PERSISTED = MetricsSystem.masterCounter("FilesPersisted");
    private static final Counter NEW_BLOCKS_GOT = MetricsSystem.masterCounter("NewBlocksGot");
    private static final Counter PATHS_DELETED = MetricsSystem.masterCounter("PathsDeleted");
    private static final Counter PATHS_MOUNTED = MetricsSystem.masterCounter("PathsMounted");
    private static final Counter PATHS_RENAMED = MetricsSystem.masterCounter("PathsRenamed");
    private static final Counter PATHS_UNMOUNTED = MetricsSystem.masterCounter("PathsUnmounted");

    // TODO(peis): Increment the RPCs OPs at the place where we receive the RPCs.
    private static final Counter COMPLETE_FILE_OPS = MetricsSystem.masterCounter("CompleteFileOps");
    private static final Counter CREATE_DIRECTORIES_OPS =
        MetricsSystem.masterCounter("CreateDirectoryOps");
    private static final Counter CREATE_FILES_OPS = MetricsSystem.masterCounter("CreateFileOps");
    private static final Counter DELETE_PATHS_OPS = MetricsSystem.masterCounter("DeletePathOps");
    private static final Counter FREE_FILE_OPS = MetricsSystem.masterCounter("FreeFileOps");
    private static final Counter GET_FILE_BLOCK_INFO_OPS =
        MetricsSystem.masterCounter("GetFileBlockInfoOps");
    private static final Counter GET_FILE_INFO_OPS = MetricsSystem.masterCounter("GetFileInfoOps");
    private static final Counter GET_NEW_BLOCK_OPS = MetricsSystem.masterCounter("GetNewBlockOps");
    private static final Counter MOUNT_OPS = MetricsSystem.masterCounter("MountOps");
    private static final Counter RENAME_PATH_OPS = MetricsSystem.masterCounter("RenamePathOps");
    private static final Counter SET_ATTRIBUTE_OPS = MetricsSystem.masterCounter("SetAttributeOps");
    private static final Counter UNMOUNT_OPS = MetricsSystem.masterCounter("UnmountOps");

    public static final String FILES_PINNED = "FilesPinned";
    public static final String PATHS_TOTAL = "PathsTotal";
    public static final String UFS_CAPACITY_TOTAL = "UfsCapacityTotal";
    public static final String UFS_CAPACITY_USED = "UfsCapacityUsed";
    public static final String UFS_CAPACITY_FREE = "UfsCapacityFree";

    /**
     * Register some file system master related gauges.
     */
    private static void registerGauges(final FileSystemMaster master) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(FILES_PINNED),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getNumberOfPinnedFiles();
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(PATHS_TOTAL),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getNumberOfPaths();
            }
          });

      final String ufsDataFolder = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
      final UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsDataFolder);

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(UFS_CAPACITY_TOTAL),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try {
                ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_TOTAL);
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return ret;
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(UFS_CAPACITY_USED),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try {
                ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_USED);
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return ret;
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(UFS_CAPACITY_FREE),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try {
                ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_FREE);
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return ret;
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }
||||||| merged common ancestors
  public List<WorkerInfo> getWorkerInfoList() {
    return mBlockMaster.getWorkerInfoList();
  }

  /**
   * This class represents the executor for periodic inode ttl check.
   */
  private final class MasterInodeTtlCheckExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link MasterInodeTtlCheckExecutor}.
     */
    public MasterInodeTtlCheckExecutor() {}

    @Override
    public void heartbeat() {
      Set<TtlBucket> expiredBuckets = mTtlBuckets.getExpiredBuckets(System.currentTimeMillis());
      for (TtlBucket bucket : expiredBuckets) {
        for (Inode inode : bucket.getInodes()) {
          AlluxioURI path = null;
          try (LockedInodePath inodePath = mInodeTree
              .lockFullInodePath(inode.getId(), InodeTree.LockMode.READ)) {
            path = inodePath.getUri();
          } catch (Exception e) {
            LOG.error("Exception trying to clean up {} for ttl check: {}", inode.toString(),
                e.toString());
          }
          if (path != null) {
            try {
              TtlAction ttlAction = inode.getTtlAction();
              LOG.debug("Path {} TTL has expired, performing action {}", path.getPath(), ttlAction);
              switch (ttlAction) {
                case FREE:
                  // public free method will lock the path, and check WRITE permission required at
                  // parent of file
                  if (inode.isDirectory()) {
                    free(path, FreeOptions.defaults().setForced(true).setRecursive(true));
                  } else {
                    free(path, FreeOptions.defaults().setForced(true));
                  }
                  // Reset state
                  inode.setTtl(Constants.NO_TTL);
                  inode.setTtlAction(TtlAction.DELETE);
                  mTtlBuckets.remove(inode);
                  break;
                case DELETE:// Default if not set is DELETE
                  // public delete method will lock the path, and check WRITE permission required at
                  // parent of file
                  if (inode.isDirectory()) {
                    delete(path, DeleteOptions.defaults().setRecursive(true));
                  } else {
                    delete(path, DeleteOptions.defaults().setRecursive(false));
                  }
                  break;
                default:
                  LOG.error("Unknown ttl action {}", ttlAction);
              }
            } catch (Exception e) {
              LOG.error("Exception trying to clean up {} for ttl check: {}", inode.toString(),
                  e.toString());
            }
          }
        }
      }
      mTtlBuckets.removeBuckets(expiredBuckets);
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Lost files periodic check.
   */
  private final class LostFilesDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostFilesDetectionHeartbeatExecutor}.
     */
    public LostFilesDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat() {
      for (long fileId : getLostFiles()) {
        // update the state
        try (LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
          Inode<?> inode = inodePath.getInode();
          if (inode.getPersistenceState() != PersistenceState.PERSISTED) {
            inode.setPersistenceState(PersistenceState.LOST);
          }
        } catch (FileDoesNotExistException e) {
          LOG.error("Exception trying to get inode from inode tree: {}", e.toString());
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Class that contains metrics for FileSystemMaster.
   * This class is public because the counter names are referenced in
   * {@link alluxio.web.WebInterfaceMasterMetricsServlet}.
   */
  public static final class Metrics {
    private static final Counter DIRECTORIES_CREATED =
        MetricsSystem.masterCounter("DirectoriesCreated");
    private static final Counter FILE_BLOCK_INFOS_GOT =
        MetricsSystem.masterCounter("FileBlockInfosGot");
    private static final Counter FILE_INFOS_GOT = MetricsSystem.masterCounter("FileInfosGot");
    private static final Counter FILES_COMPLETED = MetricsSystem.masterCounter("FilesCompleted");
    private static final Counter FILES_CREATED = MetricsSystem.masterCounter("FilesCreated");
    private static final Counter FILES_FREED = MetricsSystem.masterCounter("FilesFreed");
    private static final Counter FILES_PERSISTED = MetricsSystem.masterCounter("FilesPersisted");
    private static final Counter NEW_BLOCKS_GOT = MetricsSystem.masterCounter("NewBlocksGot");
    private static final Counter PATHS_DELETED = MetricsSystem.masterCounter("PathsDeleted");
    private static final Counter PATHS_MOUNTED = MetricsSystem.masterCounter("PathsMounted");
    private static final Counter PATHS_RENAMED = MetricsSystem.masterCounter("PathsRenamed");
    private static final Counter PATHS_UNMOUNTED = MetricsSystem.masterCounter("PathsUnmounted");

    // TODO(peis): Increment the RPCs OPs at the place where we receive the RPCs.
    private static final Counter COMPLETE_FILE_OPS = MetricsSystem.masterCounter("CompleteFileOps");
    private static final Counter CREATE_DIRECTORIES_OPS =
        MetricsSystem.masterCounter("CreateDirectoryOps");
    private static final Counter CREATE_FILES_OPS = MetricsSystem.masterCounter("CreateFileOps");
    private static final Counter DELETE_PATHS_OPS = MetricsSystem.masterCounter("DeletePathOps");
    private static final Counter FREE_FILE_OPS = MetricsSystem.masterCounter("FreeFileOps");
    private static final Counter GET_FILE_BLOCK_INFO_OPS =
        MetricsSystem.masterCounter("GetFileBlockInfoOps");
    private static final Counter GET_FILE_INFO_OPS = MetricsSystem.masterCounter("GetFileInfoOps");
    private static final Counter GET_NEW_BLOCK_OPS = MetricsSystem.masterCounter("GetNewBlockOps");
    private static final Counter MOUNT_OPS = MetricsSystem.masterCounter("MountOps");
    private static final Counter RENAME_PATH_OPS = MetricsSystem.masterCounter("RenamePathOps");
    private static final Counter SET_ATTRIBUTE_OPS = MetricsSystem.masterCounter("SetAttributeOps");
    private static final Counter UNMOUNT_OPS = MetricsSystem.masterCounter("UnmountOps");

    public static final String FILES_PINNED = "FilesPinned";
    public static final String PATHS_TOTAL = "PathsTotal";
    public static final String UFS_CAPACITY_TOTAL = "UfsCapacityTotal";
    public static final String UFS_CAPACITY_USED = "UfsCapacityUsed";
    public static final String UFS_CAPACITY_FREE = "UfsCapacityFree";

    /**
     * Register some file system master related gauges.
     */
    private static void registerGauges(final FileSystemMaster master) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(FILES_PINNED),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getNumberOfPinnedFiles();
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(PATHS_TOTAL),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getNumberOfPaths();
            }
          });

      final String ufsDataFolder = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
      final UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsDataFolder);

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(UFS_CAPACITY_TOTAL),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try {
                ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_TOTAL);
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return ret;
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(UFS_CAPACITY_USED),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try {
                ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_USED);
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return ret;
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(UFS_CAPACITY_FREE),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try {
                ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_FREE);
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return ret;
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }
=======
  List<WorkerInfo> getWorkerInfoList();
>>>>>>> fae727c2236d0c5dfe53c04006dd70509cb38b94
}
