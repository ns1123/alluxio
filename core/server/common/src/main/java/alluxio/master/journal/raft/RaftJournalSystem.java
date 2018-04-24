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

package alluxio.master.journal.raft;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.exception.JournalClosedException;
import alluxio.master.PrimarySelector;
import alluxio.master.journal.AbstractJournalSystem;
import alluxio.master.journal.AsyncJournalWriter;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.JournalEntryStateMachine;
import alluxio.master.journal.JournalWriter;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;

import com.google.common.base.Preconditions;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * System for multiplexing many logical journals into a single raft-based journal.
 *
 * This class embeds a CopycatServer which implements the raft algorithm, replicating entries across
 * a majority of servers before applying them to the state machine. To make the Copycat system work
 * as an Alluxio journal system, we implement two non-standard behaviors: (1) pre-applying
 * operations on the primary and (2) tightly controlling primary snapshotting.
 * <h1>Pre-apply</h1>
 * <p>
 * Unlike the Copycat framework, Alluxio updates state machine state *before* writing to the
 * journal. This lets us avoid journaling operations which do not result in state modification. To
 * make this work in the Copycat framework, we allow RPCs to modify state directly, then write an
 * entry to Copycat afterwards. Once the entry is journaled, Copycat will attempt to apply the
 * journal entry to each master. The entry has already been applied on the primary, so we treat all
 * journal entries applied to the primary as no-ops to avoid double-application.
 *
 * <h2>Correctness of pre-apply</h2>
 * <p>
 * There are two cases to worry about: (1) incorrectly ignoring an entry and (2) incorrectly
 * applying an entry.
 *
 * <h3> Avoid incorrectly ignoring entries</h3>
 * <p>
 * This could happen if a server thinks it is the primary, and ignores a journal entry served from
 * the real primary. To prevent this, primaries wait for a quiet period before serving requests.
 * During this time, the previous primary will go through at least two election cycles without
 * successfully sending heartbeats to the majority of the cluster. If the old primary successfully
 * sent a heartbeat to a node which elected the new primary, the old primary would realize it isn't
 * primary and step down. Therefore, the old primary will step down and no longer ignore requests by
 * the time the new primary begins sending entries.
 *
 * <h3> Avoid incorrectly applying entries</h3>
 * <p>
 * Entries can never be double-applied to a primary's state because as long as it is the primary, it
 * will ignore all entries, and once it becomes secondary, it will completely reset its state and
 * rejoin the cluster.
 *
 * <h1>Snapshot control</h1>
 * <p>
 * The way we apply journal entries to the primary makes it tricky to perform primary state snapshots.
 * Normally Copycat would decide when it wants a snapshot, but with the pre-apply protocol we may be
 * in the middle of modifying state when the snapshot would happen. To manage this, we inject an
 * AtomicBoolean into Copycat which decides whether it will be allowed to take snapshots. Normally,
 * snapshots are prohibited on the primary. However, we don't want the primary's log to grow unbounded,
 * so we allow a snapshot to be taken once a day at a user-configured time. To support this, all
 * state changes must first acquire a read lock, and snapshotting requires the corresponding write
 * lock. Once we have the write lock for all state machines, we enable snapshots in Copycat through
 * our AtomicBoolean, then wait for any snapshot to complete.
 */
@ThreadSafe
public final class RaftJournalSystem extends AbstractJournalSystem {
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalSystem.class);

  private final RaftJournalConfiguration mConf;

  // Contains all journals created by this journal system.
  private final ConcurrentHashMap<String, RaftJournal> mJournals;
  // Controls whether Copycat will attempt to take snapshots.
  private final AtomicBoolean mSnapshotAllowed;
  // Keeps track of the last time a journal entry was applied.
  private final AtomicLong mLastUpdateMs;
  // Keeps track of the sequence number of the last entry read from the journal.
  private final AtomicLong mLastReadSequenceNumber;
  private final AtomicLong mLastCommittedSequenceNumber;
  private Thread mSnapshotThread;

  private final RaftPrimarySelector mPrimarySelector;
  private final AtomicReference<CopycatClient> mClient;
  private CopycatServer mServer;

  private AsyncJournalWriter mAsyncJournalWriter;
  // Sequence numbers in the Raft journal are used for de-duplicating entries.
  // When a master gains primacy, the first entry it writes will have sequence number 0
  // and all further entries will have increasing sequence numbers.
  private final AtomicLong mNextSequenceNumber;

  /*
   * Whenever in-memory state may be inconsistent with the state represented by all flushed journal
   * entries, a read lock on this lock must be held.
   * We take a write lock when we want to perform a snapshot.
   */
  private final ReadWriteLock mJournalStateLock;

  // Whether to ignore append operations sent by Copycat. We do this when we are the primary master.
  private volatile boolean mIgnoreAppends;

  // Tracks whether we are currently performing a snapshot. Set to true while the body of
  // RaftStateMachine#snapshot() is executing.
  private volatile boolean mSnapshotting;

  /**
   * @param conf raft journal configuration
   */
  private RaftJournalSystem(RaftJournalConfiguration conf) {
    mConf = conf;
    mJournals = new ConcurrentHashMap<>();
    mSnapshotAllowed = new AtomicBoolean(true);
    mLastUpdateMs = new AtomicLong(-1);
    mLastReadSequenceNumber = new AtomicLong(-1);
    mLastCommittedSequenceNumber = new AtomicLong(-1);
    mJournalStateLock = new ReentrantReadWriteLock(true);
    mPrimarySelector = new RaftPrimarySelector();
    mClient = new AtomicReference<>();
    mAsyncJournalWriter = new AsyncJournalWriter(new RaftJournalWriter());
    mNextSequenceNumber = new AtomicLong(0);
  }

  /**
   * Creates and initializes a raft journal system.
   *
   * @param conf raft journal configuration
   * @return the created raft journal system
   */
  public static RaftJournalSystem create(RaftJournalConfiguration conf) {
    RaftJournalSystem system = new RaftJournalSystem(conf);
    system.initServer();
    system.scheduleSnapshots();
    return system;
  }

  private synchronized void initServer() {
    Preconditions.checkState(mConf.getMaxLogSize() <= Integer.MAX_VALUE,
        "{} has value {} but must not exceed {}", PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX,
        mConf.getMaxLogSize(), Integer.MAX_VALUE);
    LOG.debug("Creating journal with max segment size {}", mConf.getMaxLogSize());
    Storage storage =
        Storage.builder()
            .withDirectory(mConf.getPath())
            .withStorageLevel(StorageLevel.valueOf(mConf.getStorageLevel().name()))
            // Minor compaction happens anyway after snapshotting. We only free entries when
            // snapshotting, so there is no benefit to regular minor compaction cycles. We set a
            // high value because infinity is not allowed. If we set this too high it will overflow.
            .withMinorCompactionInterval(Duration.ofDays(200))
            .withMaxSegmentSize((int) mConf.getMaxLogSize())
            .build();
    mServer = CopycatServer.builder(getLocalAddress(mConf))
        .withStorage(storage)
        .withElectionTimeout(Duration.ofMillis(mConf.getElectionTimeoutMs()))
        .withHeartbeatInterval(Duration.ofMillis(mConf.getHeartbeatIntervalMs()))
        .withSnapshotAllowed(mSnapshotAllowed)
        .withSerializer(createSerializer())
        .withTransport(new NettyTransport())
        .withStateMachine(() -> {
          for (RaftJournal journal : mJournals.values()) {
            journal.getStateMachine().resetState();
          }
          LOG.info("Created new journal state machine");
          return new RaftStateMachine();
        })
        .build();
    mPrimarySelector.init(mServer);
  }

  private CopycatClient createClient() {
    return CopycatClient.builder(getClusterAddresses(mConf))
        .withRecoveryStrategy(RecoveryStrategies.RECOVER)
        .withConnectionStrategy(attempt -> attempt.retry(Duration.ofMillis(
            Math.min(Math.round(100D * Math.pow(2D, (double) attempt.attempt())), 1000L))))
        .build();
  }

  private void scheduleSnapshots() {
    LocalTime snapshotTime =
        LocalTime.parse(Configuration.get(PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_TIME),
            DateTimeFormatter.ISO_OFFSET_TIME);
    int dailySnapshots =
        Configuration.getInt(PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_FREQUENCY);

    mSnapshotThread =
        new Thread(new AnchoredFixedRateRunnable(snapshotTime, dailySnapshots, this::snapshot));
    mSnapshotThread.setName("alluxio-embedded-journal-snapshot");
    mSnapshotThread.setDaemon(true);
    mSnapshotThread.start();
  }

  private static List<Address> getClusterAddresses(RaftJournalConfiguration conf) {
    return conf.getClusterAddresses().stream()
        .map(addr -> new Address(addr.getHostName(), addr.getPort()))
        .collect(Collectors.toList());
  }

  private static Address getLocalAddress(RaftJournalConfiguration conf) {
    return new Address(conf.getLocalAddress().getHostName(), conf.getLocalAddress().getPort());
  }

  /**
   * Locks down all state machines to give Copycat an opportunity to take a snapshot.
   */
  private void snapshot() {
    // We only trigger manual snapshots on the primary when automatic snapshots are disabled.
    if (!mSnapshotAllowed.get()) {
      return;
    }
    LOG.info("Locking journal for daily snapshot");
    try (LockResource l = new LockResource(mJournalStateLock.writeLock())) {
      mSnapshotAllowed.set(true);
      try {
        // Submit a journal entry to trigger snapshotting.
        try {
          CopycatClient client = mClient.get();
          if (client == null) {
            // Must have lost primacy.
            return;
          }
          client.submit(new JournalEntryCommand(JournalEntry.getDefaultInstance())).get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
          LOG.error("Exception submitting empty journal entry during snapshot", e);
        }
        // Give Copycat time to initiate snapshotting. It shouldn't take more than a second, but we wait
        // 10 seconds to be on the safe side.
        CommonUtils.sleepMs(10 * Constants.SECOND_MS);
      } finally {
        mSnapshotAllowed.set(false);
        // There are a few instructions between where Copycat checks mSnapshotAllowed and invokes
        // our snapshot method. We need a short wait to make sure Copycat doesn't start snapshotting
        // after we've waited for snapshotting to finish.
        CommonUtils.sleepMs(2 * Constants.SECOND_MS);
        int timeoutMs = 60 * Constants.MINUTE_MS;
        long startMs = System.currentTimeMillis();
        CommonUtils.waitFor("snapshotting to finish", x -> !mSnapshotting,
            WaitForOptions.defaults().setTimeoutMs(timeoutMs));
        if (mSnapshotting) {
          LOG.error(
              "Failed to finish snapshot within {}ms. Interrupted: {}. System exiting to avoid corruption.",
              System.currentTimeMillis() - startMs, Thread.interrupted());
          System.exit(-1);
        }
      }
    }
    LOG.info("Journal unlocked");
  }

  /**
   * @return the serializer for commands in the {@link StateMachine}
   */
  public static Serializer createSerializer() {
    return new Serializer().register(JournalEntryCommand.class, 1);
  }

  @Override
  public Journal createJournal(JournalEntryStateMachine master) {
    RaftJournal journal = new RaftJournal(master);
    mJournals.put(master.getName(), journal);
    return journal;
  }

  @Override
  public synchronized void gainPrimacy() {
    mSnapshotAllowed.set(false);
    waitQuietPeriod();
    mIgnoreAppends = true;
    mClient.set(createClient());
    try {
      mClient.get().connect().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      String errorMessage = ExceptionMessage.FAILED_RAFT_CONNECT.getMessage(
          Arrays.toString(getClusterAddresses(mConf).toArray()), e.getCause().toString());
      throw new RuntimeException(errorMessage, e.getCause());
    }
  }

  @Override
  public synchronized void losePrimacy() {
    mIgnoreAppends = false;
    try {
      mClient.get().close().get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Failed to close raft client", e);
    } catch (TimeoutException e) {
      LOG.warn("Failed to close raft client after 10 seconds");
    }
    mClient.set(null);
    LOG.info("Shutting down Raft server");
    try {
      mServer.shutdown().get();
    } catch (ExecutionException e) {
      LOG.error(
          "Failed to leave Raft cluster while stepping down. Exiting to prevent inconsistency", e);
      System.exit(-1);
      throw new IllegalStateException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while leaving Raft cluster");
    }
    LOG.info("Shut down Raft server");
    initServer();
    LOG.info("Bootstrapping new Raft server");
    try {
      mServer.bootstrap(getClusterAddresses(mConf)).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while rejoining Raft cluster");
    } catch (ExecutionException e) {
      LOG.error("Failed to rejoin Raft cluster with addresses {} while stepping down. Exiting to "
          + "prevent inconsistency", getClusterAddresses(mConf), e);
      System.exit(-1);
    }
    LOG.info("Last sequence numbers written/committed before losing primacy: {}/{}.",
        mNextSequenceNumber.get() - 1, mLastCommittedSequenceNumber.get());
    mNextSequenceNumber.set(0);
    mLastCommittedSequenceNumber.set(-1);
    mSnapshotAllowed.set(true);
  }

  private void waitQuietPeriod() {
    long startTime = System.currentTimeMillis();
    // Sleep for quietDurationMs, then keep sleeping until we've gone quietDurationMs milliseconds
    // without applying any journal entries.
    CommonUtils.sleepMs(mConf.getQuietTimeMs());
    // Wait for any pending snapshot to finish
    CommonUtils.waitFor("any pending snapshots to finish", x -> !mSnapshotting);
    long quiet = System.currentTimeMillis() - mLastUpdateMs.get();
    while (quiet < mConf.getQuietTimeMs()) {
      CommonUtils.sleepMs(mConf.getQuietTimeMs() - quiet);
      quiet = System.currentTimeMillis() - mLastUpdateMs.get();
    }
    LOG.info("Waited in quiet period for {}ms. Last sequence number from previous term: {}",
        System.currentTimeMillis() - startTime, mLastReadSequenceNumber.get());
  }

  @Override
  public synchronized void startInternal() throws InterruptedException, IOException {
    LOG.info("Starting Raft journal system");
    long startTime = System.currentTimeMillis();
    try {
      mServer.bootstrap(getClusterAddresses(mConf)).get();
    } catch (ExecutionException e) {
      String errorMessage = ExceptionMessage.FAILED_RAFT_BOOTSTRAP.getMessage(
          Arrays.toString(getClusterAddresses(mConf).toArray()), e.getCause().toString());
      throw new IOException(errorMessage, e.getCause());
    }
    LOG.info("Started Raft Journal System in {}ms. Cluster addresses: {}. Local address: {}",
        System.currentTimeMillis() - startTime, getClusterAddresses(mConf), getLocalAddress(mConf));
  }

  @Override
  public synchronized void stopInternal() throws InterruptedException, IOException {
    LOG.info("Shutting down raft journal");
    mSnapshotThread.interrupt();
    mSnapshotThread.join(60 * Constants.SECOND_MS);
    if (mSnapshotThread.isAlive()) {
      LOG.error("Failed to stop snapshot thread");
    }
    try {
      mServer.shutdown().get();
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to shut down Raft server", e);
    }
    long lastWritten = mNextSequenceNumber.get() - 1;
    if (lastWritten >= 0) {
      LOG.info("Journal shutdown complete. Last sequence numbers written/committed in this term: "
          + "{}/{}", lastWritten, mLastCommittedSequenceNumber.get());
    }
    LOG.info("Journal shutdown complete");
  }

  @Override
  public boolean isFormatted() throws IOException {
    return mConf.getPath().exists();
  }

  @Override
  public void format() throws IOException {
    FileUtils.deletePathRecursively(mConf.getPath().getAbsolutePath());
    mConf.getPath().mkdirs();
  }

  /**
   * @return a primary selector backed by leadership within the Raft cluster
   */
  public PrimarySelector getPrimarySelector() {
    return mPrimarySelector;
  }

  /**
   * A state machine representing the state of this journal system. Entries applied to this state
   * machine will be forwarded to the appropriate internal master.
   */
  public class RaftStateMachine extends AbstractRaftStateMachine {
    @Override
    protected void resetState() {
      for (RaftJournal journal : mJournals.values()) {
        journal.getStateMachine().resetState();
      }
    }

    @Override
    protected void applyJournalEntry(JournalEntry entry) {
      mLastUpdateMs.set(System.currentTimeMillis());
      if (mIgnoreAppends) {
        // We should only ignore entries written by this master. If this condition is true, we are
        // ignoring an entry written by another master.
        long next = mNextSequenceNumber.get();
        if (next < entry.getSequenceNumber()) {
          LOG.error("Unexpected journal entry: {} applied when primary master next sequence number "
              + "is only {}. Exiting to prevent inconsistency", entry, next);
          System.exit(-1);
        }
        return;
      }
      mLastReadSequenceNumber.set(entry.getSequenceNumber());

      String masterName;
      try {
        masterName = JournalEntryAssociation.getMasterForEntry(entry);
      } catch (InvalidJournalEntryException e) {
        LOG.error("Unrecognized journal entry: {}. Exiting to prevent inconsistency", entry, e);
        System.exit(-1);
        throw new IllegalStateException(); // Proving to the compiler that control flow stops here.
      }
      try {
        JournalEntryStateMachine master = mJournals.get(masterName).getStateMachine();
        LOG.trace("Applying entry to master {}: {} ", masterName, entry);
        master.processJournalEntry(entry);
      } catch (Throwable t) {
        LOG.error("Failed to apply journal entry to master {}. Exiting to prevent inconsistency. "
            + "Entry: {}", masterName, entry, t);
        System.exit(-1);
      }
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
      LOG.debug("Calling snapshot");
      Preconditions.checkState(!mSnapshotting, "Cannot call snapshot multiple times concurrently");
      mSnapshotting = true;
      long start = System.currentTimeMillis();
      try {
        for (RaftJournal journal : mJournals.values()) {
          for (Iterator<JournalEntry> it = journal.getStateMachine().getJournalEntryIterator(); it
              .hasNext();) {
            JournalEntry entry = it.next();
            LOG.trace("Writing entry to snapshot: {}", entry);
            try {
              entry.writeDelimitedTo(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                  writer.writeByte(b);
                }

                @Override
                public void write(byte[] b, int off, int len) {
                  writer.write(b, off, len);
                }
              });
            } catch (IOException e) {
              LOG.error(
                  "Failed to take snapshot for master {}. Failed to write entry {}. Terminating "
                      + "process to prevent inconsistency.",
                  journal.getStateMachine().getName(), entry, e);
              System.exit(-1);
              // Prove to the compiler that this branch never returns.
              throw new IllegalStateException();
            }
          }
        }
        LOG.info("completed snapshot in {}ms", System.currentTimeMillis() - start);
      } catch (Throwable t) {
        LOG.error("Failed to snapshot", t);
        throw t;
      } finally {
        mSnapshotting = false;
      }
    }
  }

  /**
   * Class associated with each master that lets the master create journal contexts for writing
   * entries to the journal.
   */
  private class RaftJournal implements Journal {
    private final JournalEntryStateMachine mStateMachine;

    /**
     * @param stateMachine the state machine for this journal
     */
    public RaftJournal(JournalEntryStateMachine stateMachine) {
      mStateMachine = stateMachine;
    }

    /**
     * @return the state machine for this journal
     */
    public JournalEntryStateMachine getStateMachine() {
      return mStateMachine;
    }

    @Override
    public URI getLocation() {
      return mConf.getPath().toURI();
    }

    @Override
    public JournalContext createJournalContext() {
      return new RaftJournalContext(mAsyncJournalWriter, mJournalStateLock.readLock());
    }

    @Override
    public void close() throws IOException {
      // Nothing to close.
    }
  }

  /**
   * Class for writing entries to the Raft journal. Written entries are aggregated until flush is
   * called, then they are submitted as a single unit. This class is used only by
   * AsyncJournalWriter, which does not require its inner JournalWriter to be ThreadSafe.
   */
  @NotThreadSafe
  private class RaftJournalWriter implements JournalWriter {
    private JournalEntry.Builder mJournalEntryBuilder;

    @Override
    public void write(JournalEntry entry) throws IOException {
      Preconditions.checkState(entry.getAllFields().size() <= 1,
          "Raft journal entries should never set multiple fields, but found %s", entry);
      if (mJournalEntryBuilder == null) {
        mJournalEntryBuilder = JournalEntry.newBuilder();
      }
      mJournalEntryBuilder.addJournalEntries(
          entry.toBuilder().setSequenceNumber(mNextSequenceNumber.getAndIncrement()).build());
    }

    @Override
    public void flush() throws IOException, JournalClosedException {
      if (mJournalEntryBuilder != null) {
        try {
          CopycatClient client = mClient.get();
          if (client == null) {
            throw new JournalClosedException("Cannot write journal entry; journal has been closed");
          }
          // It is ok to submit the same entries multiple times because we de-duplicate by sequence
          // number when applying them. This could happen if a previous client and new client both
          // successfully submit the same entries.
          client.submit(new JournalEntryCommand(mJournalEntryBuilder.build())).get(10,
              TimeUnit.SECONDS);
          mLastCommittedSequenceNumber.set(mNextSequenceNumber.get() - 1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        } catch (TimeoutException e) {
          throw new IOException("Timed out after waiting 10 seconds for journal flush", e);
        }
        mJournalEntryBuilder = null;
      }
    }
  }

  /**
   * A primary selector backed by a Raft consensus cluster.
   */
  @ThreadSafe
  private static final class RaftPrimarySelector implements PrimarySelector {
    private CopycatServer mServer;
    private Listener<CopycatServer.State> mStateListener;

    private final Lock mStateLock = new ReentrantLock();
    private final Condition mStateCond = mStateLock.newCondition();
    @GuardedBy("mStateLock")
    private State mState;

    /**
     * Constructs a new {@link RaftPrimarySelector}.
     */
    private RaftPrimarySelector() {
      mState = State.SECONDARY;
    }

    /**
     * @param server reference to the server backing this selector
     */
    private void init(CopycatServer server) {
      mServer = Preconditions.checkNotNull(server, "server");
      if (mStateListener != null) {
        mStateListener.close();
      }
      // We must register the callback before initializing mState in case the state changes
      // immediately after initializing mState.
      mStateListener = server.onStateChange(state -> {
        mStateLock.lock();
        try {
          State newState = getState();
          LOG.info("Journal transitioned to state {}, Primary selector transitioning to {}", state,
              newState);
          if (mState != newState) {
            mState = newState;
            mStateCond.signalAll();
          }
        } finally {
          mStateLock.unlock();
        }
      });
      mStateLock.lock();
      try {
        mState = getState();
      } finally {
        mStateLock.unlock();
      }
    }

    private State getState() {
      if (mServer.state() == CopycatServer.State.LEADER) {
        return State.PRIMARY;
      } else {
        return State.SECONDARY;
      }
    }

    @Override
    public void start(InetSocketAddress address) throws IOException {
      // The copycat cluster is owned by the outer {@link RaftJournalSystem}.
    }

    @Override
    public void stop() throws IOException {
      // The copycat cluster is owned by the outer {@link RaftJournalSystem}.
    }

    @Override
    public void waitForState(State state) throws InterruptedException {
      mStateLock.lock();
      try {
        while (mState != state) {
          mStateCond.await();
        }
      } finally {
        mStateLock.unlock();
      }
    }
  }
}
