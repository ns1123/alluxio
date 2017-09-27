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

package alluxio.raft;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.PrimarySelector;
import alluxio.master.journal.AbstractJournalSystem;
import alluxio.master.journal.AsyncJournalWriter;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.JournalEntryStateMachine;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.raft.RaftJournalConfiguration;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.FileUtils;

import com.google.common.base.Preconditions;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
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
import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

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
 * the real primary. The real primary will wait the quiet period before serving requests. During this
 * time, the previous primary will go through at least two election cycles without successfully
 * sending heartbeats to the majority of the cluster. If the old primary successfully sent a heartbeat to a
 * node which elected the new primary, the old primary would realize it isn't primary and step down.
 * Therefore, the old primary will step down and no longer ignore requests by the time the new primary
 * begins sending entries.
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
@NotThreadSafe
public final class RaftJournalSystem extends AbstractJournalSystem {
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalSystem.class);

  private final RaftJournalConfiguration mConf;
  private final CopycatClient mClient;

  // Contains all journals created by this journal system.
  private final ConcurrentHashMap<String, RaftJournal> mJournals;
  // Controls whether Copycat will attempt to take snapshots.
  private final AtomicBoolean mSnapshotAllowed;
  // Keeps track of the last time a journal entry was applied.
  private final AtomicLong mLastUpdateMs;
  private final ScheduledExecutorService mExecutorService;

  private CopycatServer mServer;

  /*
   * Whenever in-memory state may be inconsistent with the state represented by all flushed journal
   * entries, a read lock on this lock must be held.
   * We take a write lock when we want to perform a snapshot.
   */
  private final ReadWriteLock mJournalStateLock;

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
    mClient = CopycatClient.builder(getClusterAddresses(conf))
        .withConnectionStrategy(ConnectionStrategies.EXPONENTIAL_BACKOFF).build();
    mJournalStateLock = new ReentrantReadWriteLock(true);
    mExecutorService =
        Executors.newScheduledThreadPool(1, ThreadFactoryUtils.build("raft-snapshot", true));
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

  private void initServer() {
    Preconditions.checkState(mConf.getMaxLogSize() <= Integer.MAX_VALUE,
        "{} has value {} but must not exceed {}", PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX,
        mConf.getMaxLogSize(), Integer.MAX_VALUE);
    LOG.debug("Creating journal with max segment size {}", mConf.getMaxLogSize());
    Storage storage =
        Storage.builder().withDirectory(mConf.getPath()).withStorageLevel(StorageLevel.DISK)
            // Minor compaction happens anyway after snapshotting. We only free entries when
            // snapshotting, so there is no benefit to regular minor compaction cycles. We set a
            // high value because infinity is not allowed. If we set this too high it will overflow.
            .withMinorCompactionInterval(Duration.ofDays(200))
            .withMaxSegmentSize((int) mConf.getMaxLogSize())
            .build();
    mServer = CopycatServer.builder(getLocalAddress(mConf))
        .withStorage(storage)
        .withSnapshotAllowed(mSnapshotAllowed)
        .withSerializer(createSerializer())
        .withTransport(new NettyTransport())
        .withStateMachine(() -> {
          for (RaftJournal journal : mJournals.values()) {
            journal.getStateMachine().resetState();
          }
          return new RaftStateMachine();
        })
        .build();
  }

  private void scheduleSnapshots() {
    LocalTime snapshotTime =
        LocalTime.parse(Configuration.get(PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_TIME),
            DateTimeFormatter.ISO_OFFSET_TIME);
    LocalTime now = LocalTime.now(Clock.systemUTC());
    Duration timeUntilNextSnapshot;
    if (snapshotTime.isAfter(now)) {
      timeUntilNextSnapshot = Duration.between(now, snapshotTime);
    } else {
      timeUntilNextSnapshot = Duration.ofDays(1).minus(Duration.between(snapshotTime, now));
    }
    mExecutorService.scheduleAtFixedRate(this::snapshot, timeUntilNextSnapshot.toMillis(),
        Duration.ofDays(1).toMillis(), TimeUnit.MILLISECONDS);
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
    // Only run manual snapshots on the primary.
    if (getMode().equals(Mode.SECONDARY)) {
      return;
    }
    LOG.info("Locking journal for daily snapshot");
    mJournalStateLock.writeLock().lock();
    try {
      mSnapshotAllowed.set(true);
      // Submit a journal entry to trigger snapshotting.
      try {
        mClient.submit(new alluxio.raft.JournalEntryCommand(JournalEntry.getDefaultInstance())).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOG.error("Exception submitting empty journal entry during snapshot", e);
      }
      // Give Copycat time to initiate snapshotting. It shouldn't take more than a second, but we wait
      // 10 seconds to be on the safe side.
      CommonUtils.sleepMs(10 * Constants.SECOND_MS);
      mSnapshotAllowed.set(false);
      // There are a few instructions between where Copycat checks mSnapshotAllowed and invokes
      // our snapshot method. We need a short wait to make sure Copycat doesn't start snapshotting
      // after we've waited for snapshotting to finish.
      CommonUtils.sleepMs(2 * Constants.SECOND_MS);
      CommonUtils.waitFor("snapshotting to finish", x -> !mSnapshotting);
    } finally {
      mJournalStateLock.writeLock().unlock();
    }
    LOG.info("Journal unlocked");
  }

  /**
   * @return the serializer for commands in the {@link StateMachine}
   */
  public static Serializer createSerializer() {
    return new Serializer().register(alluxio.raft.JournalEntryCommand.class, 1);
  }

  @Override
  public Journal createJournal(JournalEntryStateMachine master) {
    RaftJournal journal = new RaftJournal(master);
    mJournals.put(master.getName(), journal);
    return journal;
  }

  @Override
  public void gainPrimacy() {
    mSnapshotAllowed.set(false);
    waitQuietPeriod();
  }

  @Override
  public void losePrimacy() {
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
    initServer();
    try {
      mServer.join(getClusterAddresses(mConf)).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while rejoining Raft cluster");
    } catch (ExecutionException e) {
      LOG.error("Failed to rejoin Raft cluster with addresses {} while stepping down. Exiting to "
          + "prevent inconsistency", getClusterAddresses(mConf), e);
      System.exit(-1);
    }
    for (RaftJournal journal : mJournals.values()) {
      journal.getStateMachine().resetState();
    }
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
    LOG.info("Waited in quiet period for {}ms", System.currentTimeMillis() - startTime);
  }

  @Override
  public void startInternal() throws InterruptedException, IOException {
    LOG.info("Starting Raft journal system");
    long startTime = System.currentTimeMillis();
    try {
      mServer.bootstrap(getClusterAddresses(mConf)).get();
    } catch (ExecutionException e) {
      String errorMessage = ExceptionMessage.FAILED_RAFT_BOOTSTRAP.getMessage(
          Arrays.toString(getClusterAddresses(mConf).toArray()), e.getCause().toString());
      throw new IOException(errorMessage, e.getCause());
    }
    try {
      mClient.connect(getClusterAddresses(mConf)).get();
    } catch (ExecutionException e) {
      String errorMessage = ExceptionMessage.FAILED_RAFT_CONNECT.getMessage(
          Arrays.toString(getClusterAddresses(mConf).toArray()), e.getCause().toString());
      throw new IOException(errorMessage, e.getCause());
    }
    LOG.info("Started Raft Journal System in {}ms. Cluster addresses: {}. Local address: {}",
        System.currentTimeMillis() - startTime, getClusterAddresses(mConf), getLocalAddress(mConf));
  }

  @Override
  public void stopInternal() throws InterruptedException, IOException {
    LOG.info("Shutting down raft journal");
    mExecutorService.shutdown();
    mExecutorService.awaitTermination(1, TimeUnit.SECONDS);
    try {
      mServer.shutdown().get();
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to shut down Raft server", e);
    }
    LOG.info("Journal shut down");
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
    return new RaftPrimarySelector();
  }

  /**
   * A state machine representing the state of this journal system. Entries applied to this state
   * machine will be forwarded to the appropriate internal master.
   */
  public class RaftStateMachine extends AbstractRaftStateMachine {
    @Override
    protected void applyJournalEntry(JournalEntry entry) {
      mLastUpdateMs.set(System.currentTimeMillis());
      // The primary mutates state directly instead of going through the Raft cluster.
      if (getMode().equals(Mode.PRIMARY)) {
        return;
      }

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

  private class RaftJournal implements Journal, JournalWriter {
    private final JournalEntryStateMachine mStateMachine;
    private final AsyncJournalWriter mAsyncWriter;
    private JournalEntry.Builder mJournalEntryBuilder;

    /**
     * @param stateMachine the state machine for this journal
     */
    public RaftJournal(JournalEntryStateMachine stateMachine) {
      mStateMachine = stateMachine;
      mAsyncWriter = new AsyncJournalWriter(this);
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

    public void write(JournalEntry entry) throws IOException {
      Preconditions.checkState(entry.getAllFields().size() <= 1,
          "Raft journal entries should never set multiple fields, but found %s", entry);
      if (mJournalEntryBuilder == null) {
        mJournalEntryBuilder = JournalEntry.newBuilder();
      }
      mJournalEntryBuilder.addJournalEntries(entry);
    }

    @Override
    public void flush() throws IOException {
      if (mJournalEntryBuilder != null) {
        try {
          mClient.submit(new alluxio.raft.JournalEntryCommand(mJournalEntryBuilder.build())).get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        }
        mJournalEntryBuilder = null;
      }
    }

    @Override
    public JournalContext createJournalContext() {
      return new RaftJournalContext(mAsyncWriter, mJournalStateLock.readLock());
    }

    @Override
    public void close() throws IOException {
      // Nothing to close.
    }
  }

  /**
   * A primary selector backed by a Raft consensus cluster.
   */
  private class RaftPrimarySelector implements PrimarySelector {
    @GuardedBy("mStateLock")
    private State mState;
    private final Lock mStateLock = new ReentrantLock();
    private final Condition mStateCond = mStateLock.newCondition();

    /**
     * Constructs a new {@link RaftPrimarySelector}.
     */
    public RaftPrimarySelector() {
      // We must register the callback before initializing mState in case the state changes
      // immediately after initializing mState.
      mServer.onStateChange(state -> {
        mStateLock.lock();
        try {
          State newState = getState();
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
