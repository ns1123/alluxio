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
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private final ImmutableMap<String, UnderFileSystem> mUnderFileSystems;
  private final ExecutorService mExecutorService = Executors.newCachedThreadPool();

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
    Map<String, String> providerToUfs = new HashMap<>();
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

    // Finally create the underlying under file systems.
    Map<String, UnderFileSystem> ufses =  new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : ufsToOptions.entrySet()) {
      LOG.info(entry.getKey() + " " + entry.getValue());
      ufses.put(entry.getKey(), UnderFileSystem.Factory.create(entry.getKey(),
          UnderFileSystemConfiguration.defaults().setUserSpecifiedConf(entry.getValue())));
    }
    mUnderFileSystems = ImmutableMap.copyOf(ufses);
  }

  @Override
  public String getUnderFSType() {
    return "fork";
  }

  @Override
  public void close() throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Map.Entry<String, UnderFileSystem> entry) {
            try {
              entry.getValue().close();
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet());
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
        new Function<Pair<Map.Entry<String, UnderFileSystem>, Collection<OutputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, Collection<OutputStream>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              Collection<OutputStream> streams = arg.getValue();
              streams.add(entry.getValue().create(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), streams));
    return new ForkUnderFileOutputStream(mExecutorService, streams);
  }

  @Override
  public OutputStream create(final String path, final CreateOptions options) throws IOException {
    Collection<OutputStream> streams = new ConcurrentLinkedQueue<>();
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, Collection<OutputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, Collection<OutputStream>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              Collection<OutputStream> streams = arg.getValue();
              streams.add(entry.getValue().create(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), streams));
    return new ForkUnderFileOutputStream(mExecutorService, streams);
  }

  @Override
  public boolean deleteDirectory(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true,
                  entry.getValue().deleteDirectory(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public boolean deleteDirectory(final String path, final DeleteOptions options) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true,
                  entry.getValue().deleteDirectory(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public boolean deleteFile(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result
                  .compareAndSet(true, entry.getValue().deleteFile(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public boolean exists(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(entry.getValue().exists(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public long getBlockSizeByte(final String path) throws IOException {
    AtomicReference<Long> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Long>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Long>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Long> result = arg.getValue();
              result.set(entry.getValue().getBlockSizeByte(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(final String path) throws IOException {
    AtomicReference<UfsDirectoryStatus> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<UfsDirectoryStatus>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<UfsDirectoryStatus>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UfsDirectoryStatus> result = arg.getValue();
              result.set(entry.getValue().getDirectoryStatus(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public List<String> getFileLocations(final String path) throws IOException {
    AtomicReference<List<String>> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>,
            AtomicReference<List<String>>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<List<String>>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<List<String>> result = arg.getValue();
              result.set(entry.getValue().getFileLocations(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public List<String> getFileLocations(final String path, final FileLocationOptions options)
      throws IOException {
    AtomicReference<List<String>> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>,
            AtomicReference<List<String>>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<List<String>>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<List<String>> result = arg.getValue();
              result.set(entry.getValue().getFileLocations(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public UfsFileStatus getFileStatus(final String path) throws IOException {
    AtomicReference<UfsFileStatus> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<UfsFileStatus>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<UfsFileStatus>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UfsFileStatus> result = arg.getValue();
              result.set(entry.getValue().getFileStatus(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public long getSpace(final String path, final SpaceType type) throws IOException {
    AtomicReference<Long> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Long>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Long>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Long> result = arg.getValue();
              result.set(entry.getValue().getSpace(convert(entry.getKey(), path), type));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public boolean isDirectory(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(entry.getValue().isDirectory(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public boolean isFile(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(entry.getValue().isFile(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public UfsStatus[] listStatus(final String path) throws IOException {
    AtomicReference<UfsStatus[]> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>,
            AtomicReference<UfsStatus[]>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<UfsStatus[]>>
                  arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UfsStatus[]> result = arg.getValue();
              result.set(entry.getValue().listStatus(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public UfsStatus[] listStatus(final String path, final ListOptions options)
      throws IOException {
    AtomicReference<UfsStatus[]> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>,
            AtomicReference<UfsStatus[]>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<UfsStatus[]>>
                  arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UfsStatus[]> result = arg.getValue();
              result.set(entry.getValue().listStatus(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public boolean mkdirs(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true, entry.getValue().mkdirs(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public boolean mkdirs(final String path, final MkdirsOptions options) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true,
                  entry.getValue().mkdirs(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public InputStream open(final String path) throws IOException {
    Collection<InputStream> streams = new ConcurrentLinkedQueue<>();
    ForkUnderFileSystemUtils.invokeSome(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, Collection<InputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, Collection<InputStream>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              Collection<InputStream> streams = arg.getValue();
              streams.add(entry.getValue().open(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), streams));
    return new ForkUnderFileInputStream(mExecutorService, streams);
  }

  @Override
  public InputStream open(final String path, final OpenOptions options) throws IOException {
    Collection<InputStream> streams = new ConcurrentLinkedQueue<>();
    ForkUnderFileSystemUtils.invokeSome(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, Collection<InputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, Collection<InputStream>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              Collection<InputStream> streams = arg.getValue();
              streams.add(entry.getValue().open(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), streams));
    return new ForkUnderFileInputStream(mExecutorService, streams);
  }

  @Override
  public boolean renameDirectory(final String src, final String dst) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true, entry.getValue()
                  .renameDirectory(convert(entry.getKey(), src), convert(entry.getKey(), dst)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public boolean renameFile(final String src, final String dst) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.compareAndSet(true, entry.getValue()
                  .renameFile(convert(entry.getKey(), src), convert(entry.getKey(), dst)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, ForkUnderFileSystemUtils.fold(mUnderFileSystems.entrySet(), result));
    return result.get();
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return new AlluxioURI(ufsBaseUri.getScheme(), ufsBaseUri.getAuthority(),
        PathUtils.concatPath(ufsBaseUri.getPath(), alluxioPath), ufsBaseUri.getQueryMap());
  }

  @Override
  public void setOwner(final String path, final String user, final String group)
      throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Map.Entry<String, UnderFileSystem> arg) {
            try {
              arg.getValue().setOwner(convert(arg.getKey(), path), user, group);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet());
  }

  @Override
  public void setMode(final String path, final short mode) throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Map.Entry<String, UnderFileSystem> arg) {
            try {
              arg.getValue().setMode(convert(arg.getKey(), path), mode);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet());
  }

  @Override
  public void connectFromMaster(final String hostname) throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Map.Entry<String, UnderFileSystem> arg) {
            try {
              arg.getValue().connectFromMaster(hostname);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet());
  }

  @Override
  public void connectFromWorker(final String hostname) throws IOException {
    ForkUnderFileSystemUtils.invokeAll(mExecutorService,
        new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Map.Entry<String, UnderFileSystem> arg) {
            try {
              arg.getValue().connectFromWorker(hostname);
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet());
  }

  @Override
  public boolean supportsFlush() {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      result.set(result.get() && entry.getValue().supportsFlush());
    }
    return result.get();
  }

  private String convert(String base, String path) {
    return PathUtils.concatPath(base, (new AlluxioURI(path)).getPath());
  }
}
