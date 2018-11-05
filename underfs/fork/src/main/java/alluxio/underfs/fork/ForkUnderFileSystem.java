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

package alluxio.underfs.fork;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.SyncInfo;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Virtual implementation of {@link UnderFileSystem} which can be used to fork write operations
 * to multiple UFSes.
 */
@ThreadSafe
public class ForkUnderFileSystem implements UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(ForkUnderFileSystem.class);
  private static final Pattern OPTION_PATTERN =
      Pattern.compile("alluxio-fork\\.(.+)\\.option\\.(.+)");
  private static final Pattern UFS_PATTERN = Pattern.compile("alluxio-fork\\.(.+)\\.ufs");

  private final ImmutableMap<String, Pair<String, UnderFileSystem>> mUnderFileSystems;
  private final ExecutorService mExecutorService = Executors.newCachedThreadPool();
  private final List<String> mPhysicalStores = new ArrayList<>();

  /**
   * Constructs a new {@link ForkUnderFileSystem}.
   *
   * @param ufsConf the UFS configuration
   */
  ForkUnderFileSystem(UnderFileSystemConfiguration ufsConf) {
    // the ufs configuration is expected to contain the following keys:
    // - alluxio-fork.<PROVIDER>.ufs - specifies the nested UFS URIs
    // - alluxio-fork.<PROVIDER>.option.<KEY> - specifies the UFS-specific options
    //
    // In the first pass we identify all providers.
    Map<String, String> providerToUfs = new TreeMap<>();
    Map<String, Map<String, String>> ufsToOptions = new HashMap<>();
    for (Map.Entry<String, String> entry : ufsConf.getUserSpecifiedConf().entrySet()) {
      Matcher matcher = UFS_PATTERN.matcher(entry.getKey());
      if (matcher.matches()) {
        providerToUfs.put(matcher.group(1), entry.getValue());
        ufsToOptions.put(entry.getValue(), new HashMap<String, String>());
      }
    }

    // In the second pass we collect options for each provider.
    for (Map.Entry<String, String> entry : ufsConf.getUserSpecifiedConf().entrySet()) {
      Matcher matcher = OPTION_PATTERN.matcher(entry.getKey());
      if (matcher.matches()) {
        String ufs = providerToUfs.get(matcher.group(1));
        Map<String, String> options = ufsToOptions.get(ufs);
        options.put(matcher.group(2), entry.getValue());
      }
    }

    // In the the third pass we report all unrecognized properties.
    for (Map.Entry<String, String> entry : ufsConf.getUserSpecifiedConf().entrySet()) {
      Matcher optionMatcher = OPTION_PATTERN.matcher(entry.getKey());
      Matcher ufsMatcher = UFS_PATTERN.matcher(entry.getKey());
      if (!optionMatcher.matches() && !ufsMatcher.matches()) {
        LOG.warn("Unrecognized property {}={}", entry.getKey(), entry.getValue());
      }
    }

    // Finally create the underlying under file systems, sorted by the name of their provider.
    Map<String, Pair<String, UnderFileSystem>> ufses = new TreeMap<>();
    for (Map.Entry<String, String> entry : providerToUfs.entrySet()) {
      String provider = entry.getKey();
      String ufs = entry.getValue();
      mPhysicalStores.add(new AlluxioURI(ufs).getRootPath());
      Map<String, String> options = ufsToOptions.get(ufs);
      LOG.debug("provider={} ufs={} options={}", provider, ufs, options);
      ufses.put(provider, new ImmutablePair<>(ufs, UnderFileSystem.Factory
          .create(ufs, UnderFileSystemConfiguration.defaults().setUserSpecifiedConf(options))));
    }

    mUnderFileSystems = ImmutableMap.copyOf(ufses);
  }

  @Override
  public String getUnderFSType() {
    return "fork";
  }

  @Override
  public void cleanup() throws IOException {
  }

  @Override
  public void close() throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<String, UnderFileSystem> entry) {
            try {
              entry.getValue().close();
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.values());
    try {
      mExecutorService.shutdown();
      if (!mExecutorService.awaitTermination(1, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Failed to shutdown executor service");
      }
    } catch (InterruptedException e) {
      mExecutorService.shutdownNow();
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public OutputStream create(final String path) throws IOException {
    Collection<OutputStream> streams = new ConcurrentLinkedQueue<>();
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Pair<String, UnderFileSystem>, Collection<OutputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, Collection<OutputStream>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              Collection<OutputStream> streams = arg.getValue();
              streams.add(entry.getValue().create(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), streams));
    return new ForkUnderFileOutputStream(mExecutorService, streams);
  }

  @Override
  public OutputStream create(final String path, final CreateOptions options) throws IOException {
    Collection<OutputStream> streams = new ConcurrentLinkedQueue<>();
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Pair<String, UnderFileSystem>, Collection<OutputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, Collection<OutputStream>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              Collection<OutputStream> streams = arg.getValue();
              streams.add(entry.getValue().create(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), streams));
    return new ForkUnderFileOutputStream(mExecutorService, streams);
  }

  @Override
  public boolean deleteDirectory(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true,
                  entry.getValue().deleteDirectory(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public boolean deleteDirectory(final String path, final DeleteOptions options) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true,
                  entry.getValue().deleteDirectory(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public boolean deleteFile(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result
                  .compareAndSet(true, entry.getValue().deleteFile(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public boolean exists(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(entry.getValue().exists(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public alluxio.collections.Pair<AccessControlList, DefaultAccessControlList> getAclPair(
      String path) throws IOException {
    Collection<alluxio.collections.Pair<AccessControlList, DefaultAccessControlList>> result
        = new ConcurrentLinkedQueue<>();
    try {
      ForkUnderFileSystemUtils.invokeAll(mExecutorService,
          new Function<Pair<Pair<String, UnderFileSystem>,
              Collection<alluxio.collections.Pair<AccessControlList, DefaultAccessControlList>>>,
              IOException>() {
            @Nullable
            @Override
            public IOException apply(
                Pair<Pair<String, UnderFileSystem>,
                    Collection<alluxio.collections.Pair<AccessControlList,
                        DefaultAccessControlList>>> arg) {
              try {
                Pair<String, UnderFileSystem> entry = arg.getKey();
                Collection<alluxio.collections.Pair<AccessControlList,
                    DefaultAccessControlList>> result
                    = arg.getValue();
                result.add(entry.getValue().getAclPair(convert(entry.getKey(), path)));
              } catch (IOException e) {
                return e;
              }
              return null;
            }
          }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    } catch (IOException e) {
      return new alluxio.collections.Pair<>(null, null);
    }

    // If one of the getAcl result is non-null, we return that
    for (alluxio.collections.Pair<AccessControlList, DefaultAccessControlList> aclPair: result) {
      if (aclPair != null && aclPair.getFirst() != null) {
        return aclPair;
      }
    }

    return null;
  }

  @Override
  public long getBlockSizeByte(final String path) throws IOException {
    AtomicReference<Long> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Long>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Long>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Long> result = arg.getValue();
              result.set(entry.getValue().getBlockSizeByte(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(final String path) throws IOException {
    AtomicReference<UfsDirectoryStatus> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<UfsDirectoryStatus>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<UfsDirectoryStatus>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UfsDirectoryStatus> result = arg.getValue();
              result.set(entry.getValue().getDirectoryStatus(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public List<String> getFileLocations(final String path) throws IOException {
    AtomicReference<List<String>> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>,
            AtomicReference<List<String>>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<List<String>>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<List<String>> result = arg.getValue();
              result.set(entry.getValue().getFileLocations(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public List<String> getFileLocations(final String path, final FileLocationOptions options)
      throws IOException {
    AtomicReference<List<String>> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>,
            AtomicReference<List<String>>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<List<String>>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<List<String>> result = arg.getValue();
              result.set(entry.getValue().getFileLocations(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public UfsFileStatus getFileStatus(final String path) throws IOException {
    AtomicReference<UfsFileStatus> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<UfsFileStatus>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<UfsFileStatus>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UfsFileStatus> result = arg.getValue();
              result.set(entry.getValue().getFileStatus(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public String getFingerprint(String path) {
    Collection<String> result = new ConcurrentLinkedQueue<>();
    try {
      ForkUnderFileSystemUtils.invokeAll(mExecutorService,
          new Function<Pair<Pair<String, UnderFileSystem>, Collection<String>>,
              IOException>() {
            @Nullable
            @Override
            public IOException apply(
                Pair<Pair<String, UnderFileSystem>, Collection<String>> arg) {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              Collection<String> result = arg.getValue();
              result.add(entry.getValue().getFingerprint(convert(entry.getKey(), path)));
              return null;
            }
          }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    } catch (IOException e) {
      return Constants.INVALID_UFS_FINGERPRINT;
    }

    // Combine all the fingerprints into the content hash.
    StringBuilder contentHash = new StringBuilder();
    Fingerprint fingerprint = null;
    for (String fp : result) {
      if (Constants.INVALID_UFS_FINGERPRINT.equals(fp)) {
        // There was an error in one of the fingerprints.
        return Constants.INVALID_UFS_FINGERPRINT;
      }
      if (fingerprint == null) {
        fingerprint = Fingerprint.parse(fp);
      }
      contentHash.append(fp);
    }
    if (fingerprint == null) {
      return Constants.INVALID_UFS_FINGERPRINT;
    }
    fingerprint.putTag(Fingerprint.Tag.CONTENT_HASH, contentHash.toString());
    return fingerprint.serialize();
  }

  @Override
  public long getSpace(final String path, final SpaceType type) throws IOException {
    AtomicReference<Long> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Long>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Long>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Long> result = arg.getValue();
              result.set(entry.getValue().getSpace(convert(entry.getKey(), path), type));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    if (isFile(path)) {
      return getFileStatus(path);
    }
    return getDirectoryStatus(path);
  }

  @Override
  public boolean isDirectory(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(entry.getValue().isDirectory(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public boolean isFile(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(entry.getValue().isFile(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
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
    AtomicReference<UfsStatus[]> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>,
            AtomicReference<UfsStatus[]>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<UfsStatus[]>>
                  arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UfsStatus[]> result = arg.getValue();
              result.set(entry.getValue().listStatus(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public UfsStatus[] listStatus(final String path, final ListOptions options)
      throws IOException {
    AtomicReference<UfsStatus[]> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>,
            AtomicReference<UfsStatus[]>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<UfsStatus[]>>
                  arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UfsStatus[]> result = arg.getValue();
              result.set(entry.getValue().listStatus(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public boolean mkdirs(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true, entry.getValue().mkdirs(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public boolean mkdirs(final String path, final MkdirsOptions options) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true,
                  entry.getValue().mkdirs(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public InputStream open(final String path) throws IOException {
    List<Pair<String, UnderFileSystem>> ufses = new ArrayList<>();
    for (Pair<String, UnderFileSystem> entry : mUnderFileSystems.values()) {
      String root = entry.getLeft();
      UnderFileSystem ufs = entry.getRight();
      ufses.add(new ImmutablePair<>(convert(root, path), ufs));
    }
    return new ForkUnderFileInputStream(ufses, OpenOptions.defaults());
  }

  @Override
  public InputStream open(final String path, final OpenOptions options) throws IOException {
    List<Pair<String, UnderFileSystem>> ufses = new ArrayList<>();
    for (Pair<String, UnderFileSystem> entry : mUnderFileSystems.values()) {
      String root = entry.getLeft();
      UnderFileSystem ufs = entry.getRight();
      ufses.add(new ImmutablePair<>(convert(root, path), ufs));
    }
    return new ForkUnderFileInputStream(ufses, options);
  }

  @Override
  public boolean renameDirectory(final String src, final String dst) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true, entry.getValue()
                  .renameDirectory(convert(entry.getKey(), src), convert(entry.getKey(), dst)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public boolean renameFile(final String src, final String dst) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Pair<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true, entry.getValue()
                  .renameFile(convert(entry.getKey(), src), convert(entry.getKey(), dst)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return new AlluxioURI(ufsBaseUri.getScheme(), ufsBaseUri.getAuthority(),
        PathUtils.concatPath(ufsBaseUri.getPath(), alluxioPath), ufsBaseUri.getQueryMap());
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<String, UnderFileSystem> arg) {
            try {
              arg.getValue().setAclEntries(convert(arg.getKey(), path), aclEntries);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.values());
  }

  @Override
  public void setOwner(final String path, final String user, final String group)
      throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<String, UnderFileSystem> arg) {
            try {
              arg.getValue().setOwner(convert(arg.getKey(), path), user, group);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.values());
  }

  @Override
  public void setMode(final String path, final short mode) throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<String, UnderFileSystem> arg) {
            try {
              arg.getValue().setMode(convert(arg.getKey(), path), mode);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.values());
  }

  @Override
  public void connectFromMaster(final String hostname) throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<String, UnderFileSystem> arg) {
            try {
              arg.getValue().connectFromMaster(hostname);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.values());
  }

  @Override
  public void connectFromWorker(final String hostname) throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<String, UnderFileSystem> arg) {
            try {
              arg.getValue().connectFromWorker(hostname);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.values());
  }

  @Override
  public boolean supportsFlush() {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    for (Pair<String, UnderFileSystem> entry : mUnderFileSystems.values()) {
      result.set(result.get() && entry.getValue().supportsFlush());
    }
    return result.get();
  }

  @Override
  public boolean supportsActiveSync() {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    for (Pair<String, UnderFileSystem> entry : mUnderFileSystems.values()) {
      result.set(result.get() && entry.getValue().supportsActiveSync());
    }
    return result.get();
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    for (Pair<String, UnderFileSystem> entry : mUnderFileSystems.values()) {
      result.set(result.get() && entry.getValue().startActiveSyncPolling(txId));
    }
    return result.get();
  }

  @Override
  public boolean stopActiveSyncPolling() throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    for (Pair<String, UnderFileSystem> entry : mUnderFileSystems.values()) {
      result.set(result.get() && entry.getValue().stopActiveSyncPolling());
    }
    return result.get();
  }

  @Override
  public SyncInfo getActiveSyncInfo() throws IOException {
    AtomicReference<SyncInfo> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Pair<String, UnderFileSystem>,
            AtomicReference<SyncInfo>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Pair<String, UnderFileSystem>, AtomicReference<SyncInfo>>
                  arg) {
              try {
                Pair<String, UnderFileSystem> entry = arg.getKey();
                AtomicReference<SyncInfo> result = arg.getValue();
                result.set(entry.getValue().getActiveSyncInfo());
              } catch (IOException e) {
                return e;
              }
              return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.values(), result));
    return result.get();
  }

  @Override
  public void startSync(AlluxioURI uri) throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<String, UnderFileSystem> arg) {
            try {
              arg.getValue().startSync(uri);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.values());
  }

  @Override
  public void stopSync(AlluxioURI uri) throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Pair<String, UnderFileSystem> arg) {
            try {
              arg.getValue().stopSync(uri);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.values());
  }

  private String convert(String base, String path) {
    return PathUtils.concatPath(base, (new AlluxioURI(path)).getPath());
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
    if (noAccessCount == mPhysicalStores.size()) {
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
}
