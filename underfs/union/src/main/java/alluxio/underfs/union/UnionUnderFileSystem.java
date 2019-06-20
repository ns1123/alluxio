/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.SyncInfo;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsMode;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Virtual implementation of {@link UnderFileSystem} which can be used to unify the namespaces of
 * a number of different {@link UnderFileSystem}
 *
 * Operations against this UFS support filesystem paths in the form of URIs:
 * {@code  [{scheme}://{authority}]/path/to/file} where the {@code scheme} and {@code authority}
 * fields are optional. The {@code scheme} must always be specified as "{@code union}" and the
 * {@code authority} may reference an existing alias within the unified union namespace. This can
 * be used to force certain operations on this UFS to act
 *
 * When a specific UFS alias is not passed in the authority of the target path, all reads from this
 * virtual UFS will respect the ordering of
 * {@code alluxio-union.priority.read}. Writes to this UFS will write to the collection of UFSes
 * within {@code alluxio-union.collection.create}.
 *
 * Otherwise, operations which require write permissions and the existence of entities such as
 * {@code setAttribute}, {@code rename}, {@code delete}, will attempt to act on all underlying
 * UFSes within this virtual UFS. In the case of errors or {@code FileNotFoundException}, they
 * will be ignored and these operations on this UFS will return successfully if it is the case
 * that the operation on at least one of the underlying UFSes succeeded.
 *
 */
@ThreadSafe
public class UnionUnderFileSystem implements UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(UnionUnderFileSystem.class);

  public static final String SCHEME = "union";

  // alluxio-union.<ALIAS>.uri - specifies the uri for the given ALIAS
  private static final Pattern ALIAS_URI =
      Pattern.compile("alluxio-union\\.(.+)\\.uri");

  // alluxio-union.priority.read=ALIAS_1,ALIAS_2,ALIAS_3,... - determines read priority of UFS
  // List is ordered highest priority to lowest priority
  // All aliases must be specified
  // No duplicates
  private static final String READ_PRIORITY = "alluxio-union.priority.read";

  // alluxio-union.collection.create=ALIAS_1,ALIAS_2,ALIAS_3,... - determines UFSs written to
  // List does not need to contain all UFS aliases
  // If a UFS is included in the collection, all writes will write to it.
  // No duplicates
  private static final String CREATE_COLLECTION = "alluxio-union.collection.create";

  // alluxio-union.<ALIAS>.option.<KEY>=<VALUE> - specifies the UFS-specific options
  private static final Pattern UFS_OPTION =
      Pattern.compile("alluxio-union\\.(.+)\\.option\\.(.+)");

  /** Set of exception class names to ignore. */
  private static final Set<String> IGNORE_EXCEPTIONS =
      Sets.newHashSet(FileNotFoundException.class.getName(), NoSuchFileException.class.getName());

  private static final Pattern AUTHORITY_ALIAS_RE =
      Pattern.compile("^(?<authority>[^\\(]*)(\\((?<alias>.*)\\))?$");

  static final IndexDefinition<UfsKey, String> ALIAS_IDX =
      new IndexDefinition<UfsKey, String>(true) {
    @Override
    public String getFieldValue(UfsKey o) {
      return o.getAlias();
    }
  };

  static final IndexDefinition<UfsKey, Integer> PRIORITY_IDX =
      new IndexDefinition<UfsKey, Integer>(true) {
    @Override
    public Integer getFieldValue(UfsKey o) {
      return o.getPriority();
    }
  };

  private final ExecutorService mExecutorService;
  private final IndexedSet<UfsKey> mUfses;
  private final Set<UfsKey> mCreateUfses;
  private Map<Integer, PersistenceState> mUfsHint = null;

  /** Physical stores are the root URIs of each UFS in the union. Ex: {@code shceme://host:port/} */
  private final List<String> mPhysicalStores = new ArrayList<>();

  /**
   * Constructs a new {@link UnionUnderFileSystem}.
   *
   * @param ufsConf the UFS configuration
   */
  UnionUnderFileSystem(UnderFileSystemConfiguration ufsConf) {
    Map<String, String> mountConf = ufsConf.getMountSpecificConf();
    Map<String, String> aliasToUri = new HashMap<>();
    List<String> readPriority = null;
    List<String> createCollection = null; // Not really a priority, more like a set.
    Map<String, Map<String, String>> ufsOptions = new HashMap<>(); //

    // First, must to determine all of the aliases
    // We can initialize each entry for the UFS options as well.
    for (Map.Entry<String, String> entry: mountConf.entrySet()) {
      Matcher matcher = ALIAS_URI.matcher(entry.getKey());
      if (matcher.matches()) {
        String alias = matcher.group(1);
        Preconditions.checkState(alias.length() > 0,
            String.format("Alias must not be empty string from %s", entry.getKey()));
        aliasToUri.put(alias, entry.getValue());
        ufsOptions.put(alias, new HashMap<>());
      }
    }
    Preconditions.checkState(aliasToUri.size() > 0,
        String.format("At least one UFS must be specified with %s", ALIAS_URI));

    // Next, get the read priority - must contain all previously defined aliases
    if (mountConf.containsKey(READ_PRIORITY)) {
      readPriority = Lists.newArrayList(Splitter.on(",").trimResults().omitEmptyStrings()
          .split(mountConf.get(READ_PRIORITY)));
    }
    Preconditions.checkArgument(readPriority != null,
        String.format("The option %s must be provided", READ_PRIORITY));
    Preconditions.checkArgument(aliasToUri.size() == readPriority.size(), String.format(
            "The number of items in %s=%s must match the number of aliases provided by %s",
            READ_PRIORITY, readPriority, "alluxio-union.<ALIAS>.uri"));
    // Check for duplicates
    Preconditions.checkState(new HashSet<>(readPriority).size() == readPriority.size(),
        String.format("Duplicate aliases found in read priority %s", readPriority));

    // Get the create priorities, must be specified and contain at least one UFS.
    // TODO(zac): Support omitting this property and copy the read priority/utilize all UFSs?
    if (mountConf.containsKey(CREATE_COLLECTION)) {
      createCollection = Lists.newArrayList(Splitter.on(",").trimResults().omitEmptyStrings()
          .split(mountConf.get(CREATE_COLLECTION)));
    }

    if (!ufsConf.isReadOnly()) {
      Preconditions.checkNotNull(createCollection,
          String.format("The property %s must be specified", CREATE_COLLECTION));
      Preconditions.checkArgument(createCollection.size() > 0,
          String.format("At least one UFS alias must be specified for %s", CREATE_COLLECTION));
      // Check for duplicates
      Preconditions.checkState(new HashSet<>(createCollection).size() == createCollection.size(),
          String.format("Duplicate aliases found in create priority %s", createCollection));

      if (!createCollection.contains(readPriority.get(0))) {
        // Log a warning if the UFS in the highest read priority is not contained in the create
        // priority. This could lead to possibly degraded performance
        LOG.warn("The UFS with the highest read priority ({}) is not contained within the set of "
                + "create UFSes ({}). This may lead to degraded performance.", readPriority.get(0),
            createCollection.toString());
      }
    }

    for (Map.Entry<String, String> entry: mountConf.entrySet()) {
      Matcher matcher = UFS_OPTION.matcher(entry.getKey());
      if (matcher.matches()) {
        String ufs = matcher.group(1);
        String key = matcher.group(2);
        Preconditions.checkArgument(aliasToUri.containsKey(ufs),
            String.format("UFS alias %s defined in %s does not have a URI associated with it.",
                ufs, entry.getKey()));
        ufsOptions.get(ufs).put(key, entry.getValue());
      }
    }

    mUfses = new IndexedSet<>(PRIORITY_IDX, ALIAS_IDX);
    mCreateUfses = createCollection == null
        ? Collections.emptySet() : new HashSet<>(createCollection.size());
    mExecutorService = Executors.newCachedThreadPool();

    // Lastly, add to the set indexes
    for (int i = 0; i < readPriority.size(); i++) {
      String alias = readPriority.get(i);
      String uri = aliasToUri.get(alias);
      Map<String, String> options = ufsOptions.get(alias);
      UnderFileSystem ufs = new URIInjectingUnderFileSystem(
          UnderFileSystem.Factory.create(uri, ufsConf.createMountSpecificConf(options)), uri);
      UfsKey k = new UfsKey(alias, i, ufs);
      mPhysicalStores.add(new AlluxioURI(uri).getRootPath());
      mUfses.add(k);
      // If the create UFS was specified, add it to the create set
      if (createCollection.contains(alias)) {
        mCreateUfses.add(new UfsKey(alias, -1, ufs)); // priority doesn't matter for create
      }
    }
  }

  /**
   * Copies the {@link UnionUnderFileSystem} to a new instance.
   *
   * @param other
   */
  private UnionUnderFileSystem(UnionUnderFileSystem other) {
    mUfses = other.mUfses;
    mCreateUfses = other.mCreateUfses;
    mExecutorService = other.mExecutorService;
    mUfsHint = other.mUfsHint;
  }

  /**
   * Create a view of the UFS that utilizes the given hints to optimize performance.
   *
   * Note that hints are only valid for one path, so the UFS returned from this call will only
   * benefit performance for the path on which the hint is valid.
   *
   * Any UFSes with a state of {@link PersistenceState#PERSISTED} within the hint will be
   * attempted first. If any of those calls return successfully, the result from the UFS with
   * the highest priority will return first. If the underlying UFSs are out of sync from the given
   * hint, then even if the result from the hints fail, the rest of the UFSes will be attempted
   * before returning.
   *
   * If all UFSes return an error, an error will be thrown. If at least one UFS succeeds using
   * the hint, then the function should return successfully.
   *
   * @param hints a priority to persistence state mapping
   * @return an {@link UnionUnderFileSystem} that is optimized using the given hints
   */
  public UnionUnderFileSystem from(Map<Integer, PersistenceState> hints) {
    UnionUnderFileSystem ufs = new UnionUnderFileSystem(this);
    ufs.mUfsHint = hints;
    return ufs;
  }

  /**
   * @return a pair of sets. The first first pair is the set of UFSes from the hint (or All UFSes
   *         if the hint is null). The second set is the set of UFSes not included from the hint
   */
  private Pair<Collection<UfsKey>, Collection<UfsKey>> splitWithHint() {
    if (mUfsHint == null) {
      return Pair.of(new HashSet<>(mUfses), new HashSet<>());
    }

    Set<UfsKey> hintUfs = new TreeSet<>();
    Set<UfsKey> leftOverUfs = new TreeSet<>(mUfses);

    for (Map.Entry<Integer, PersistenceState> e : mUfsHint.entrySet()) {
      if (e.getValue() != PersistenceState.PERSISTED) {
        continue;
      }
      // get the key with the given priority
      if (!mUfses.contains(PRIORITY_IDX, e.getKey())) {
        LOG.warn("UFS with priority {} not contained in Union Ufs", e.getKey());
      } else {
        // Only one UFS should be associated with a given priority.
        UfsKey k = mUfses.getFirstByField(PRIORITY_IDX, e.getKey());
        hintUfs.add(k);
        leftOverUfs.remove(k);
      }
    }
    return Pair.of(hintUfs, leftOverUfs);
  }

  @Override
  public String getUnderFSType() {
    return SCHEME;
  }

  @Override
  public void cleanup() {
  }

  @Override
  public void close() throws IOException {
    // Do nothing, since this instance is shared
  }

  @Override
  public OutputStream create(final String path) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mCreateUfses);
    Collection<OutputStream> streams = new ConcurrentSet<>();
    try {
      UnionUnderFileSystemUtils.invokeAll(
          mExecutorService,
          (ufsKey) -> streams.add(ufsKey.getUfs().create(path)),
          inputs);
    } catch (IOException e) {
      for (OutputStream s : streams) {
        s.close();
      }
      throw e;
    }
    return new UnionUnderFileOutputStream(mExecutorService, streams);
  }

  @Override
  public OutputStream create(final String path, final CreateOptions options) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mCreateUfses);
    Collection<OutputStream> streams = new ConcurrentSet<>();
    try {
      UnionUnderFileSystemUtils.invokeAll(mExecutorService,
          (ufsKey) -> streams.add(ufsKey.getUfs().create(path, options)),
          inputs);
    } catch (IOException e) {
      for (OutputStream s : streams) {
        s.close();
      }
      throw e;
    }

    return new UnionUnderFileOutputStream(mExecutorService, streams);
  }

  @Override
  public OutputStream createNonexistingFile(final String path) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mCreateUfses);
    Collection<OutputStream> streams = new ConcurrentSet<>();
    try {
      UnionUnderFileSystemUtils.invokeAll(mExecutorService,
          (ufsKey) -> streams.add(ufsKey.getUfs().createNonexistingFile(path)),
          inputs);
    } catch (IOException e) {
      for (OutputStream s : streams) {
        s.close();
      }
      throw e;
    }
    return new UnionUnderFileOutputStream(mExecutorService, streams);
  }

  @Override
  public OutputStream createNonexistingFile(final String path, CreateOptions options)
      throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mCreateUfses);
    Collection<OutputStream> streams = new ConcurrentSet<>();
    try {
      UnionUnderFileSystemUtils.invokeAll(mExecutorService,
          (ufsKey) -> streams.add(ufsKey.getUfs().createNonexistingFile(path, options)),
          inputs);
    } catch (IOException e) {
      for (OutputStream s : streams) {
        s.close();
      }
    }
    return new UnionUnderFileOutputStream(mExecutorService, streams);
  }

  @Override
  public boolean deleteDirectory(final String path) throws IOException {
    return deleteDirectory(path, DeleteOptions.defaults());
  }

  @Override
  public boolean deleteDirectory(final String path, final DeleteOptions options)
      throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      if (ufsKey.getUfs().deleteDirectory(path, options)) {
        success.incrementAndGet();
      }
    }, inputs);
    return success.get() == inputs.size();
  }

  @Override
  public boolean deleteExistingDirectory(final String path) throws IOException {
    return deleteExistingDirectory(path, DeleteOptions.defaults());
  }

  @Override
  public boolean deleteExistingDirectory(final String path, final DeleteOptions options) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      if (ufsKey.getUfs().deleteExistingDirectory(path, options)) {
        success.incrementAndGet();
      }
    }, inputs);
    return success.get() == inputs.size();
  }

  @Override
  public boolean deleteFile(final String path) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      if (ufsKey.getUfs().deleteFile(path)) {
        success.incrementAndGet();
      }
    }, inputs);
    return success.get() == inputs.size();
  }

  @Override
  public boolean deleteExistingFile(final String path) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      if (ufsKey.getUfs().deleteExistingFile(path)) {
        success.incrementAndGet();
      }
    }, inputs);
    return success.get() == inputs.size();
  }

  @Override
  public boolean exists(final String path) throws IOException {
    Pair<Collection<UfsKey>, Collection<UfsKey>> inputs = getUfsInputs(path, splitWithHint());
    // Use atomic reference instead because if null, we'll know if
    // all calls to the underlying UFS failed. Using just an atomic boolean
    // doesn't give all of the necessary information
    AtomicReference<Boolean> exists = new AtomicReference<>(null);
    List<IOException> errs = new ArrayList<>(2);
    UncheckedIOConsumer<UfsKey> ufsFunc = ufsKey -> {
      boolean res = ufsKey.getUfs().exists(path);
      exists.compareAndSet(null, res); // Set if it hasn't been set yet
      exists.compareAndSet(false, res); // Overwrite with res if false
    };
    try {
      UnionUnderFileSystemUtils.invokeSome(
          mExecutorService, ufsFunc, inputs.getLeft());
    } catch (IOException e) {
      errs.add(e);
    }

    // exists is true, we can return early
    if (exists.get() != null && exists.get()) {
      return exists.get();
    }

    try {
      UnionUnderFileSystemUtils.invokeSome(
          mExecutorService, ufsFunc, inputs.getRight());
    } catch (IOException e) {
      errs.add(e);
    }

    // still null here, all UFS calls failed.
    // Otherwise, return the current status
    if (exists.get() == null) {
      throw new AggregateException(errs);
    }
    return exists.get();
  }

  @Override
  public alluxio.collections.Pair<AccessControlList, DefaultAccessControlList> getAclPair(
      String path) throws IOException {
    return getHighestReadOpFromHint(path, (ufsKey) -> ufsKey.getUfs().getAclPair(path));
  }

  @Override
  public long getBlockSizeByte(final String path) throws IOException {
    return Optional.ofNullable(
        getHighestReadOpFromHint(path,
            ufsKey -> ufsKey.getUfs().getBlockSizeByte(path)))
        .orElseThrow(() -> new IOException("Failed to get block size from all UFSes in the union"));
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(final String path) throws IOException {
    ConcurrentSkipListMap<UfsKey, UfsDirectoryStatus> results  =
        getReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getDirectoryStatus(path));
    UfsDirectoryStatus status = UnionUnderFileSystemUtils.mergeUfsDirStatusXAttr(results);
    if (status == null) {
      throw new IOException(
          String.format("No UFS directory statuses could be retrieved at %s", path));
    }
    return status;
  }

  @Override
  public UfsDirectoryStatus getExistingDirectoryStatus(final String path) throws IOException {
    ConcurrentSkipListMap<UfsKey, UfsDirectoryStatus> results  =
        getReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getExistingDirectoryStatus(path));
    UfsDirectoryStatus status = UnionUnderFileSystemUtils.mergeUfsDirStatusXAttr(results);
    if (status == null) {
      throw new IOException(
          String.format("No existing UFS directory statuses could be retrieved at %s", path));
    }
    return status;
  }

  @Override
  public List<String> getFileLocations(final String path) throws IOException {
    List<String> locations =
        getHighestReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getFileLocations(path));
    if (locations == null) {
      throw new IOException(
          String.format("Couldn't obtain any file locations at %s", path));
    }
    return locations;
  }

  @Override
  public List<String> getFileLocations(final String path, final FileLocationOptions options)
      throws IOException {
    List<String> locations =
        getHighestReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getFileLocations(path, options));
    if (locations == null) {
      throw new IOException(
          String.format("Couldn't obtain any file locations with options at %s", path));
    }
    return locations;
  }

  @Override
  public UfsFileStatus getFileStatus(final String path) throws IOException {
    ConcurrentSkipListMap<UfsKey, UfsFileStatus> results  =
        getReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getFileStatus(path));
    UfsFileStatus status = UnionUnderFileSystemUtils.mergeUfsFileStatusXAttr(results);
    if (status == null) {
      throw new IOException(
          String.format("Couldn't obtain any FileStatus at %s", path));
    }
    return status;
  }

  @Override
  public UfsFileStatus getExistingFileStatus(final String path) throws IOException {
    ConcurrentSkipListMap<UfsKey, UfsFileStatus> results  =
        getReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getExistingFileStatus(path));
    UfsFileStatus status = UnionUnderFileSystemUtils.mergeUfsFileStatusXAttr(results);
    if (status == null) {
      throw new IOException(
          String.format("Couldn't obtain any existing FileStatus at %s", path));
    }
    return status;
  }

  @Override
  public String getFingerprint(String path) {
    String output = Constants.INVALID_UFS_FINGERPRINT;
    try {
      ConcurrentSkipListMap<UfsKey, String> results =
          getReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getFingerprint(path));
      // In a sorted map, result collection is ordered w.r.t. keys
      for (String s : results.values()) {
        output = s;
        if (!output.equals(Constants.INVALID_UFS_FINGERPRINT)) {
          break;
        }
      }
    } catch (IOException e) {
      LOG.debug("Couldn't obtain UFS fingerprint: {}", e.getMessage());
      output = Constants.INVALID_UFS_FINGERPRINT;
    }
    return output;
  }

  @Override
  public long getSpace(final String path, final SpaceType type) throws IOException {
    Long space = getHighestReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getSpace(path, type));
    if (space == null) {
      throw new IOException(
          String.format("Couldn't determine space for path %s", path));
    }
    return space;
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    ConcurrentSkipListMap<UfsKey, UfsStatus> results  =
        getReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getStatus(path));
    UfsStatus status = UnionUnderFileSystemUtils.mergeUfsStatusXAttr(results);
    if (status == null) {
      throw new IOException(String.format("Failed to get UfsStatus at %s", path));
    }
    return status;
  }

  @Override
  public UfsStatus getExistingStatus(String path) throws IOException {
    ConcurrentSkipListMap<UfsKey, UfsStatus> results  =
        getReadOpFromHint(path, ufsKey -> ufsKey.getUfs().getExistingStatus(path));
    UfsStatus status = UnionUnderFileSystemUtils.mergeUfsStatusXAttr(results);
    if (status == null) {
      throw new IOException(String.format("Failed to get existing UfsStatus at %s", path));
    }
    return status;
  }

  @Override
  public boolean isDirectory(final String path) throws IOException {
    Boolean isDir = getHighestReadOpFromHint(path, ufsKey -> ufsKey.getUfs().isDirectory(path));
    if (isDir == null) {
      throw new IOException(String.format("Failed to check if isDir at %s", path));
    }
    return isDir;
  }

  @Override
  public boolean isExistingDirectory(final String path) throws IOException {
    Boolean isDir =
        getHighestReadOpFromHint(path, ufsKey -> ufsKey.getUfs().isExistingDirectory(path));
    if (isDir == null) {
      throw new IOException(String.format("Failed to check if isExistingDir at %s", path));
    }
    return isDir;
  }

  @Override
  public boolean isFile(final String path) throws IOException {
    Boolean isFile = getHighestReadOpFromHint(path, ufsKey -> ufsKey.getUfs().isFile(path));
    if (isFile == null) {
      throw new IOException(String.format("Failed to check if isFile at %s", path));
    }
    return isFile;
  }

  @Override
  public boolean isObjectStorage() {
    return false;
  }

  @Override
  public boolean isSeekable() {
    return false;
  }

  @Override
  public UfsStatus[] listStatus(final String path) throws IOException {
    ConcurrentSkipListMap<UfsKey, UfsStatus[]> results =
        getReadOpFromHint(path, ufsKey -> ufsKey.getUfs().listStatus(path));
    return UnionUnderFileSystemUtils.mergeListStatusXAttr(results);
  }

  @Override
  public UfsStatus[] listStatus(final String path, final ListOptions options)
      throws IOException {
    ConcurrentSkipListMap<UfsKey, UfsStatus[]> results =
        getReadOpFromHint(path, ufsKey -> ufsKey.getUfs().listStatus(path, options));
    return UnionUnderFileSystemUtils.mergeListStatusXAttr(results);
  }

  @Override
  public boolean mkdirs(final String path) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mCreateUfses);
    AtomicInteger mkdirs = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      boolean res = ufsKey.getUfs().mkdirs(path);
      if (res) {
        mkdirs.incrementAndGet();
      }
    }, inputs);
    return mkdirs.get() == mCreateUfses.size();
  }

  @Override
  public boolean mkdirs(final String path, final MkdirsOptions options) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mCreateUfses);
    AtomicInteger mkdirs = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      boolean res = ufsKey.getUfs().mkdirs(path, options);
      if (res) {
        mkdirs.incrementAndGet();
      }
    }, inputs);
    return mkdirs.get() == mCreateUfses.size();
  }

  @Override
  public InputStream open(final String path) throws IOException {
    Pair<Collection<UfsKey>, Collection<UfsKey>> inputs = getUfsInputs(path, splitWithHint());
    return new UnionUnderFileInputStream(path, inputs.getLeft(), inputs.getRight(),
        OpenOptions.defaults());
  }

  @Override
  public InputStream open(final String path, final OpenOptions options) throws IOException {
    Pair<Collection<UfsKey>, Collection<UfsKey>> inputs = getUfsInputs(path, splitWithHint());
    return new UnionUnderFileInputStream(path, inputs.getLeft(), inputs.getRight(), options);
  }

  @Override
  public InputStream openExistingFile(final String path) throws IOException {
    Pair<Collection<UfsKey>, Collection<UfsKey>> inputs = getUfsInputs(path, splitWithHint());
    return new UnionUnderFileInputStream(path, inputs.getLeft(), inputs.getRight(),
        OpenOptions.defaults());
  }

  @Override
  public InputStream openExistingFile(final String path, final OpenOptions options) throws IOException {
    Pair<Collection<UfsKey>, Collection<UfsKey>> inputs = getUfsInputs(path, splitWithHint());
    return new UnionUnderFileInputStream(path, inputs.getLeft(), inputs.getRight(), options);
  }

  @Override
  public boolean renameDirectory(final String src, final String dst) throws IOException {
    Collection<UfsKey> inputs = getTwoPathUfsInputs(src, dst, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      ufsKey.getUfs().renameDirectory(src, dst);
      success.incrementAndGet();
    }, inputs, IGNORE_EXCEPTIONS);
    return success.get() > 0;
  }

  @Override
  public boolean renameRenamableDirectory(final String src, final String dst) throws IOException {
    Collection<UfsKey> inputs = getTwoPathUfsInputs(src, dst, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      ufsKey.getUfs().renameRenamableDirectory(src, dst);
      success.incrementAndGet();
    }, inputs, IGNORE_EXCEPTIONS);
    return success.get() > 0;
  }

  @Override
  public boolean renameFile(final String src, final String dst) throws IOException {
    Collection<UfsKey> inputs = getTwoPathUfsInputs(src, dst, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      ufsKey.getUfs().renameFile(src, dst);
      success.incrementAndGet();
    }, inputs, IGNORE_EXCEPTIONS);
    return success.get() > 0;
  }

  @Override
  public boolean renameRenamableFile(final String src, final String dst) throws IOException {
    Collection<UfsKey> inputs = getTwoPathUfsInputs(src, dst, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      ufsKey.getUfs().renameRenamableFile(src, dst);
      success.incrementAndGet();
    }, inputs, IGNORE_EXCEPTIONS);
    return success.get() > 0;
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return new AlluxioURI(ufsBaseUri, PathUtils.concatPath(ufsBaseUri.getPath(), alluxioPath),
        false);
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      ufsKey.getUfs().setAclEntries(path, aclEntries);
      success.incrementAndGet();
    }, inputs);
    if (success.get() <= 0) {
      throw new IOException(String.format("Failed to set ACL entries on %s", path));
    }
  }

  @Override
  public void setOwner(final String path, final String user, final String group)
      throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      ufsKey.getUfs().setOwner(path, user, group);
      success.incrementAndGet();
    }, inputs, IGNORE_EXCEPTIONS);
    if (success.get() <= 0) {
      throw new IOException(String.format("Failed to set owner on %s", path));
    }
  }

  @Override
  public void setMode(final String path, final short mode) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(path, mUfses);
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      ufsKey.getUfs().setMode(path, mode);
      success.incrementAndGet();
    }, inputs, IGNORE_EXCEPTIONS);
    if (success.get() <= 0) {
      throw new IOException(String.format("Failed to set mode on %s", path));
    }
  }

  @Override
  public void connectFromMaster(final String hostname) throws IOException {
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      ufsKey.getUfs().connectFromMaster(hostname);
      success.incrementAndGet();
    }, mUfses);
    if (success.get() <= 0) {
      throw new IOException(String.format("Failed to set connect from master to %s", hostname));
    }
  }

  @Override
  public void connectFromWorker(final String hostname) throws IOException {
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      ufsKey.getUfs().connectFromWorker(hostname);
      success.incrementAndGet();
    }, mUfses);
    if (success.get() <= 0) {
      throw new IOException(String.format("Failed to set connect from worker to %s", hostname));
    }
  }

  @Override
  public boolean supportsFlush() throws IOException {
    AtomicInteger success = new AtomicInteger(0);
    UnionUnderFileSystemUtils.invokeAll(mExecutorService, (ufsKey) -> {
      if (ufsKey.getUfs().supportsFlush()) {
        success.incrementAndGet();
      }
    }, mUfses);
    return success.get() == mUfses.size();
  }

  @Override
  public boolean supportsActiveSync() {
    return false;
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    throw new IOException("Union UFS does not support ActiveSync");
  }

  @Override
  public boolean stopActiveSyncPolling() throws IOException {
    throw new IOException("Union UFS does not support ActiveSync");
  }

  @Override
  public SyncInfo getActiveSyncInfo() throws IOException {
    throw new IOException("Union UFS does not support ActiveSync");
  }

  @Override
  public void startSync(AlluxioURI uri) throws IOException {
    throw new IOException("Union UFS does not support ActiveSync");
  }

  @Override
  public void stopSync(AlluxioURI uri) throws IOException {
    throw new IOException("Union UFS does not support ActiveSync");
  }

  @Override
  public UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState) {
    int noAccessCount = 0;
    int readOnlyCount = 0;
    for (String physicalStore : mPhysicalStores) {
      if (!physicalUfsState.containsKey(physicalStore)) {
        continue;
      }
      switch (physicalUfsState.get(physicalStore)) {
        case NO_ACCESS:
          noAccessCount++;
          break;
        case READ_ONLY:
          readOnlyCount++;
          break;
        default:
          break;
      }
    }
    if (noAccessCount == mUfses.size()) {
      // All physical stores are down
      return UfsMode.NO_ACCESS;
    }
    if (noAccessCount > 0 || readOnlyCount > 0) {
      // At least one physical store is not writable
      return UfsMode.READ_ONLY;
    }
    return UfsMode.READ_WRITE;
  }

  @Override
  public List<String> getPhysicalStores() {
    return new ArrayList<>(mPhysicalStores);
  }

  /**
   * Runs a function over two sets of inputs, returning early if there is at least one valid
   * output from the first set.
   *
   * For all of the inputs in each set, the RPCs are run concurrently. The result returned is
   * from the highest priority UFS.
   *
   * @param path the path that will be operated on
   * @param function a function which accepts a {@link UfsKey} and returns <tt>T</tt>
   * @param <T> The type of object to return
   * @return the result from the first, or highest priority, {@link UfsKey}
   * @throws IOException if all calls fail
   */
  @Nullable
  <T> T getHighestReadOpFromHint(String path, UncheckedIOFunction<UfsKey, T> function) throws IOException {
    ConcurrentSkipListMap<UfsKey, T> outputs = getReadOpFromHint(path, function);
    // Returns the item with the highest priority.
    if (outputs.isEmpty()) {
      throw new IOException("All calls to underlying UFSes failed");
    }

    return outputs.firstEntry() == null ? null : outputs.firstEntry().getValue();
  }

  /**
   * Runs a function over two sets of inputs, returning early if there is at least one valid
   * output from the first set.
   *
   * For all of the inputs in each set, the RPCs are run concurrently. The result returned is
   * from the highest priority UFS.
   *
   * A path may be passed such as {@code union://{ufs_alias}/path/to/file} which will then only
   * run the output from the UFS with {@code {ufs_alias}}
   *
   * @param path the path that will be operated on
   * @param function a function which accepts a {@link UfsKey} and returns <tt>T</tt>
   * @param <T> The type of object to return
   * @return the result map from
   * @throws IOException if all calls fail
   */
  <T> ConcurrentSkipListMap<UfsKey, T> getReadOpFromHint(String path, UncheckedIOFunction<UfsKey,
      T> function) throws IOException {
    ConcurrentSkipListMap<UfsKey, T> outputs = new ConcurrentSkipListMap<>();
    Pair<Collection<UfsKey>, Collection<UfsKey>> inputs = getUfsInputs(path, splitWithHint());
    UnionUnderFileSystemUtils.splitInvoke(UnionUnderFileSystemUtils::invokeSome,
        mExecutorService, (ufsKey) -> {
          T t = function.apply(ufsKey);
          if (t != null) {
            outputs.put(ufsKey, t);
          }
        }, outputs::isEmpty, inputs);
    return outputs;
  }

  /**
   * Take an input path and return the set of UFSs that should be used to perform the operation.
   *
   * This method is used to handles input paths such as {@code union://UfsA/path/to/file} where the
   * authority in the input URI specifies a specific UFS alias to write to in the union.
   *
   * If the UFS alias specified in the authority does not match an existing alias, an exception is
   * thrown. If there is no authority in the given URI (i.e. the input path is
   * {@code union:///} or {@code /path/to/file}) then the {@code defaultInputs} parameter will be
   * returned.
   *
   * @param inputPath the input path to a UFS function
   * @param defaultInputs the inputs to return if the input path does not specify an URI with a
   *                      UFS alias
   * @return a singleton collection of the UFS if the {@code inputPath} specifies an alias;
   *         Otherwise {@code defaultInputs}
   * @see URI
   */
  @VisibleForTesting
  Collection<UfsKey> getUfsInputs(String inputPath, Collection<UfsKey> defaultInputs)
      throws IOException {
    URI uri = URI.create(inputPath);
    // If no scheme/authority is passed, output is null
    if (uri.getScheme() == null || uri.getAuthority() == null) {
      return defaultInputs;
    } else if (!uri.getScheme().equals(SCHEME)) {
      throw new IOException(String.format("Scheme %s must only be %s in Union UFS",
          uri.getScheme(), SCHEME));
    }

    // If no authority is passed, authority is null
    String ufsAlias = uri.getAuthority();
    Matcher matcher = AUTHORITY_ALIAS_RE.matcher(ufsAlias);
    if (!matcher.matches()) {
      throw new IOException(
          String.format("Authority is not of the form \"<authority>(<alias>)\": %s", ufsAlias));
    }
    ufsAlias = matcher.group("alias");
    if (ufsAlias == null || ufsAlias.isEmpty()) {
      return defaultInputs;
    }
    UfsKey k = mUfses.getFirstByField(ALIAS_IDX, ufsAlias);
    if (k == null) {
      throw new IOException(String.format("No alias %s found in union namespace", ufsAlias));
    }
    return Collections.singleton(k);
  }

  /**
   * Takes a pair of UFSes and returns only the UFSes necessary to perform on given the operation
   * URI.
   *
   * If the URI contains a {@code {scheme}://{authority}} portion, and the {@code authority}
   * matches the alias of one of the UnionUFS, then the pair returned is simply (UFS_ALIAS, Empty).
   *
   * Otherwise, the default inputs parameter is returned.
   *
   * @param inputPath the input path to operate on
   * @param defaultInputs the default inputs if the path does not operate on a specific UFS
   * @return the pair of UFSes to operate on
   * @throws IOException when the URI alias match can't be found
   * @see #getUfsInputs(String, Collection)
   */
  Pair<Collection<UfsKey>, Collection<UfsKey>> getUfsInputs(String inputPath,
      Pair<Collection<UfsKey>, Collection<UfsKey>> defaultInputs) throws IOException {
    Collection<UfsKey> a = getUfsInputs(inputPath, defaultInputs.getLeft());
    // Purposefully doing pointer comparison to see if the collection returned is the same
    if (a != defaultInputs.getLeft()) {
      // Returned a new singleton collection for a specific UFS
      // This should be the only UFS attempted if a union://alias/ URI is specified
      return Pair.of(a, Collections.emptyList());
    } else {
      return defaultInputs;
    }
  }

  /**
   * Checks that operations which require two paths operate on the same set of UFS (i.e. rename),
   * otherwise and error is thrown.
   *
   * @param pathOne first path of the operation
   * @param pathTwo second path of the oepration
   * @param defaultInputs the inputs to return if a UFS alias isn't specific in the URI path
   * @return the input {@link UfsKey}s to use for the two-path operation
   * @throws IOException when URI aliases can't be found
   * @see #getUfsInputs(String, Collection)
   */
  Collection<UfsKey> getTwoPathUfsInputs(String pathOne, String pathTwo,
      Collection<UfsKey> defaultInputs) throws IOException {
    Collection<UfsKey> inputs = getUfsInputs(pathOne, defaultInputs);
    // The user passed a specific alias if the pointer to defaultInputs matches the input result
    if (inputs != defaultInputs) {
      // The alias must be contained within dst and src and dst alias _must_ match
      String srcAuth = URI.create(pathOne).getAuthority();
      Preconditions.checkState(srcAuth.equals(URI.create(pathTwo).getAuthority()),
          "mismatch authority: src: %s dst: %s", pathOne, pathTwo);
    }
    return inputs;
  }
}
