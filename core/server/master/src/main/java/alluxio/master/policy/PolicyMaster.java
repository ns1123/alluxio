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

package alluxio.master.policy;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.AddPolicyPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ListPolicyPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.PolicyInfo;
import alluxio.grpc.RemovePolicyPOptions;
import alluxio.grpc.ServiceType;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeView;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.policy.action.ActionScheduler;
import alluxio.master.policy.action.SchedulableAction;
import alluxio.master.policy.action.data.DataActionDefinition;
import alluxio.master.policy.action.data.DataActionUtils;
import alluxio.master.policy.action.data.DataState;
import alluxio.master.policy.cond.LogicalAnd;
import alluxio.master.policy.cond.LogicalNot;
import alluxio.master.policy.cond.OlderThan;
import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.PolicyDefinition;
import alluxio.master.policy.meta.PolicyEvaluator;
import alluxio.master.policy.meta.PolicyScope;
import alluxio.master.policy.meta.PolicyStore;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.StreamUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * This manages and enforces policies.
 */
public final class PolicyMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(PolicyMaster.class);
  private static final Set<Class<? extends Server>> DEPS = ImmutableSet.of(FileSystemMaster.class);
  private static final long POLICY_SCAN_INTERVAL =
      ServerConfiguration.getMs(PropertyKey.POLICY_SCAN_INTERVAL);
  private static final long POLICY_SCAN_INITIAL_DELAY =
      ServerConfiguration.getMs(PropertyKey.POLICY_SCAN_INITIAL_DELAY);
  private static final long ACTION_SCHEDULE_AHEAD_TIME = POLICY_SCAN_INTERVAL * 2;
  private static final Pattern SHORTCUT_RE =
      Pattern.compile("^(?<shortcut>\\w+)\\((?<body>.*)\\)$");

  /** The file system master to enforce policies on. */
  private final FileSystemMaster mFileSystemMaster;
  /** Manages the policy definitions. */
  private final PolicyStore mPolicyStore;
  /** manages the scheduling of actions and the exection of the actions. */
  private final ActionScheduler mActionScheduler;

  /** All journaled components for this master. */
  private final List<Journaled> mJournaledComponents;
  /** Evaluates the policy conditions for all events and inodes. */
  private final PolicyEvaluator mPolicyEvaluator;
  /** The journal sink for examining all the journal events. */
  private final PolicyJournalSink mPolicyJournalSink;

  /** The scheduler for periodic inode scanning. */
  private final ScheduledExecutorService mScanExecutor = Executors.newSingleThreadScheduledExecutor(
      ThreadFactoryUtils.build("PolicyMaster-InodeScan-%d", true));

  /**
   * Creates a new instance of {@link PolicyMaster}.
   *
   * @param registry the master registry
   * @param masterContext the context for Alluxio master
   */
  public PolicyMaster(MasterRegistry registry, MasterContext masterContext) {
    super(masterContext, new SystemClock(),
        ExecutorServiceFactories.cachedThreadPool(Constants.POLICY_MASTER_NAME));
    mFileSystemMaster = registry.get(FileSystemMaster.class);
    mPolicyStore = new PolicyStore();

    mPolicyEvaluator = new PolicyEvaluator(mPolicyStore);
    mActionScheduler = new ActionScheduler(mPolicyEvaluator, mFileSystemMaster, masterContext);

    mJournaledComponents = new ArrayList<>(Arrays.asList(
        mPolicyStore,
        mActionScheduler
    ));

    mPolicyJournalSink = new PolicyJournalSink(mPolicyEvaluator, mActionScheduler);
    masterContext.getJournalSystem().addJournalSink(mFileSystemMaster, mPolicyJournalSink);

    registry.add(PolicyMaster.class, this);
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>(2);
    services.put(ServiceType.POLICY_MASTER_CLIENT_SERVICE,
        new GrpcService(new PolicyMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.POLICY_MASTER_NAME;
  }

  @Override
  public boolean processJournalEntry(alluxio.proto.journal.Journal.JournalEntry entry) {
    for (Journaled journaled : mJournaledComponents) {
      if (journaled.processJournalEntry(entry)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void resetState() {
    mJournaledComponents.forEach(Journaled::resetState);
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    mActionScheduler.start(isLeader, getExecutorService());
    if (!isLeader) {
      return;
    }
    LOG.info("Starting {}", getName());

    mScanExecutor.scheduleAtFixedRate(this::scanInodes,
        POLICY_SCAN_INITIAL_DELAY, POLICY_SCAN_INTERVAL, TimeUnit.MILLISECONDS);

    LOG.info("{} is started", getName());
  }

  @Override
  public void stop() throws IOException {
    mActionScheduler.stop();
    super.stop();
  }

  @Override
  public synchronized Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    List<Iterator<Journal.JournalEntry>> componentIters = StreamUtils
        .map(JournalEntryIterable::getJournalEntryIterator, mJournaledComponents);
    return Iterators.concat(componentIters.iterator());
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.POLICY_MASTER;
  }

  /**
   * Lists the policies.
   *
   * @param options the options
   * @return the list of policy definitions
   */
  public List<PolicyInfo> listPolicy(ListPolicyPOptions options) {
    Iterator<PolicyDefinition> it = mPolicyStore.getPolicies();
    return Streams.stream(it).map(PolicyDefinition::toPolicyInfo).filter(
        pi -> {
          // only return policies which the user has read access to
          try {
            mFileSystemMaster.getFileInfo(new AlluxioURI(pi.getPath()), GetStatusContext.create(
                GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
          } catch (AccessControlException e) {
            // does not have read access to this path
            return false;
          } catch (Exception e) {
            // still return the policy if there is a non-access-control issue with the path
          }
          return true;
        }).collect(Collectors.toList());
  }

  /**
   * Gets the policy of a specific name.
   *
   * @param name name of the policy
   * @return the policy, or null if there is no policy of such name
   */
  @VisibleForTesting
  @Nullable
  public PolicyDefinition getPolicy(String name) {
    return mPolicyStore.getPolicy(name);
  }

  /**
   * Adds a policy.
   *
   * @param alluxioPath the Alluxio path to add the policy for
   * @param definition the policy definition
   * @param options the options
   */
  public void addPolicy(String alluxioPath, String definition, AddPolicyPOptions options)
      throws InvalidPathException, InvalidArgumentException, UnavailableException {
    if (alluxioPath == null || alluxioPath.isEmpty()) {
      throw new InvalidPathException("Alluxio path cannot be empty");
    }
    if (definition == null || definition.isEmpty()) {
      throw new InvalidArgumentException("Policy definition cannot be empty");
    }
    definition = definition.trim();

    Matcher matcher = SHORTCUT_RE.matcher(definition.toLowerCase());
    if (!matcher.matches()) {
      // TODO(gpang): implement DSL later
      throw new InvalidArgumentException(
          "Policy definition not supported: " + definition);
    }

    String shortcut = matcher.group("shortcut");
    String body = matcher.group("body");

    // TODO(gpang): create the policy while holding the inode lock. requires new FSM api.

    if (shortcut.toLowerCase().equals("ufsmigrate")) {
      // TODO(gpang): Is there a need for shortcuts to be a first class citizen?
      // "ufsMigrate(1y, UFS[SUBNAME1]:STORE, UFS[SUBNAME3]:REMOVE, ...)"

      String[] parts = body.split(",");
      if (parts.length < 2) {
        throw new InvalidArgumentException(
            "Policy ufsMigrate must be in the form: ufsMigrate(<older than time period>, "
                + "<location>:<operation>, ...) definition: "
                + definition);
      }
      // parse the body starting after the older than time period
      List<DataActionDefinition.LocationOperation> ops =
          DataActionUtils.deserializeBody(body.substring(body.indexOf(',') + 1));

      // TODO(gpang): get rid of id, because (name, created time) should be primary key.

      try (JournalContext journalContext = createJournalContext()) {
        mPolicyStore.add(journalContext,
            new PolicyDefinition(CommonUtils.getCurrentMs(), "ufsMigrate-" + alluxioPath,
                alluxioPath, PolicyScope.RECURSIVE, new LogicalAnd(
                Arrays.asList(new OlderThan(parts[0]), new LogicalNot(new DataState(ops)))),
                new DataActionDefinition(ops), CommonUtils.getCurrentMs()));
      }
    } else {
      throw new InvalidArgumentException(
          "Policy definition shortcut not supported: " + definition);
    }
  }

  /**
   * Removes a policy.
   *
   * @param policyName the name of the policy to remove
   * @param options the options
   */
  public void removePolicy(String policyName, RemovePolicyPOptions options)
      throws UnavailableException {
    // TODO(gpang): check for write permissions on the inode. Requires new FSM api.
    try (JournalContext journalContext = createJournalContext()) {
      mPolicyStore.remove(journalContext, policyName);
    }
  }

  private void scanInodes() {
    mFileSystemMaster.scan((path, inodeView) -> {
      InodeState inode = new InodeViewInodeState(inodeView);
      Interval interval = Interval.before(System.currentTimeMillis() + ACTION_SCHEDULE_AHEAD_TIME);
      List<SchedulableAction> actions = mPolicyEvaluator.getActions(interval, path, inode);
      mActionScheduler.scheduleActions(path, inode.getId(), actions);
    });
  }

  /**
   * An implementation of InodeState based on {@link Inode}.
   */
  private static final class InodeViewInodeState implements InodeState {
    private final InodeView mInode;

    /**
     * Creates an InodeState whose states are retrieved from inode.
     * Updates to the inode instance after construction will be reflected in the state.
     *
     * @param inode the inode to retrieve state from
     */
    public InodeViewInodeState(InodeView inode) {
      mInode = inode;
    }

    @Override
    public long getCreationTimeMs() {
      return mInode.getCreationTimeMs();
    }

    @Override
    public long getId() {
      return mInode.getId();
    }

    @Override
    public long getLastModificationTimeMs() {
      return mInode.getLastModificationTimeMs();
    }

    @Override
    public String getName() {
      return mInode.getName();
    }

    @Nullable
    @Override
    public Map<String, byte[]> getXAttr() {
      return mInode.getXAttr();
    }

    @Override
    public boolean isDirectory() {
      return mInode.isDirectory();
    }

    @Override
    public boolean isFile() {
      return mInode.isFile();
    }

    @Override
    public boolean isPersisted() {
      return mInode.isPersisted();
    }
  }
}
