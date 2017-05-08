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
import alluxio.underfs.UnderFileStatus;
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
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private final UnderFileSystemConfiguration mUfsConf;
  private final Map<String, UnderFileSystem> mUnderFileSystems = new HashMap<>();

  /**
   * Constructs a new {@link ForkUnderFileSystem}.
   *
   * @param ufsConf the UFS configuration
   */
  ForkUnderFileSystem(UnderFileSystemConfiguration ufsConf) {
    mUfsConf = ufsConf;

    // the ufs configuration is expected to contain the following keys:
    // - ufs.<PROVIDER>.alluxio - specifies the Alluxio path
    // - ufs.<PROVIDER>.ufs - specifies the nested UFS path
    // - ufs.<PROVIDER>.option.<KEY> - specifies the UFS-specific options
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

    // Finally create the underlying under file systems.
    for (Map.Entry<String, Map<String, String>> entry : ufsToOptions.entrySet()) {
      LOG.info(entry.getKey() + " " + entry.getValue());
      mUnderFileSystems
          .put(entry.getKey(), UnderFileSystem.Factory.create(entry.getKey(), entry.getValue()));
    }
  }

  @Override
  public String getUnderFSType() {
    return "multi";
  }

  @Override
  public void close() throws IOException {
    ForkUnderFileSystemUtils
        .invokeAll(new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
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
  }

  @Override
  public void configureProperties() throws IOException {
    ForkUnderFileSystemUtils
        .invokeAll(new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
          @Nullable
          @Override
          public IOException apply(Map.Entry<String, UnderFileSystem> entry) {
            try {
              entry.getValue().configureProperties();
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet());
  }

  @Override
  public OutputStream create(final String path) throws IOException {
    List<OutputStream> streams = new ArrayList<>();
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, List<OutputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, List<OutputStream>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              List<OutputStream> streams = arg.getValue();
              streams.add(entry.getValue().create(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), streams);
    return new ForkUnderFileOutputStream(streams);
  }

  @Override
  public OutputStream create(final String path, final CreateOptions options) throws IOException {
    List<OutputStream> streams = new ArrayList<>();
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, List<OutputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, List<OutputStream>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              List<OutputStream> streams = arg.getValue();
              streams.add(entry.getValue().create(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), streams);
    return new ForkUnderFileOutputStream(streams);
  }

  @Override
  public boolean deleteDirectory(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(
                  result.get() && entry.getValue().deleteDirectory(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public boolean deleteDirectory(final String path, final DeleteOptions options) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(result.get() && entry.getValue()
                  .deleteDirectory(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public boolean deleteFile(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(
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
                  .set(result.get() && entry.getValue().deleteFile(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
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
        }, mUnderFileSystems.entrySet(), result);
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
        }, mUnderFileSystems.entrySet(), result);
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
        }, mUnderFileSystems.entrySet(), result);
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
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public long getFileSize(final String path) throws IOException {
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
              result.set(entry.getValue().getFileSize(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public long getModificationTimeMs(final String path) throws IOException {
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
              result.set(entry.getValue().getModificationTimeMs(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public Map<String, String> getProperties() {
    return mUfsConf.getUserSpecifiedConf();
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
        }, mUnderFileSystems.entrySet(), result);
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
        }, mUnderFileSystems.entrySet(), result);
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
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public UnderFileStatus[] listStatus(final String path) throws IOException {
    AtomicReference<UnderFileStatus[]> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>,
            AtomicReference<UnderFileStatus[]>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<UnderFileStatus[]>>
                  arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UnderFileStatus[]> result = arg.getValue();
              result.set(entry.getValue().listStatus(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public UnderFileStatus[] listStatus(final String path, final ListOptions options)
      throws IOException {
    AtomicReference<UnderFileStatus[]> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>,
            AtomicReference<UnderFileStatus[]>>, IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<UnderFileStatus[]>>
                  arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<UnderFileStatus[]> result = arg.getValue();
              result.set(entry.getValue().listStatus(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public boolean mkdirs(final String path) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(result.get() && entry.getValue().mkdirs(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public boolean mkdirs(final String path, final MkdirsOptions options) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(
                  result.get() && entry.getValue().mkdirs(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public InputStream open(final String path) throws IOException {
    List<InputStream> streams = new ArrayList<>();
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, List<InputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, List<InputStream>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              List<InputStream> streams = arg.getValue();
              streams.add(entry.getValue().open(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), streams);
    return new ForkUnderFileInputStream(streams);
  }

  @Override
  public InputStream open(final String path, final OpenOptions options) throws IOException {
    List<InputStream> streams = new ArrayList<>();
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, List<InputStream>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, List<InputStream>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              List<InputStream> streams = arg.getValue();
              streams.add(entry.getValue().open(convert(entry.getKey(), path), options));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), streams);
    return new ForkUnderFileInputStream(streams);
  }

  @Override
  public boolean renameDirectory(final String src, final String dst) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(result.get() && entry.getValue()
                  .renameDirectory(convert(entry.getKey(), src), convert(entry.getKey(), dst)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public boolean renameFile(final String src, final String dst) throws IOException {
    AtomicReference<Boolean> result = new AtomicReference<>(true);
    ForkUnderFileSystemUtils.invokeAll(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Boolean>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Boolean> result = arg.getValue();
              result.set(result.get() && entry.getValue()
                  .renameFile(convert(entry.getKey(), src), convert(entry.getKey(), dst)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return new AlluxioURI(ufsBaseUri.getScheme(), ufsBaseUri.getAuthority(),
        PathUtils.concatPath(ufsBaseUri.getPath(), alluxioPath), ufsBaseUri.getQueryMap());
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    // TODO(jiri): decide what the semantics should be
  }

  @Override
  public void setOwner(final String path, final String user, final String group)
      throws IOException {
    ForkUnderFileSystemUtils
        .invokeAll(new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
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
    ForkUnderFileSystemUtils
        .invokeAll(new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
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
  public String getOwner(final String path) throws IOException {
    AtomicReference<String> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<String>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<String>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<String> result = arg.getValue();
              result.set(entry.getValue().getOwner(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public String getGroup(final String path) throws IOException {
    AtomicReference<String> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<String>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<String>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<String> result = arg.getValue();
              result.set(entry.getValue().getGroup(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public short getMode(final String path) throws IOException {
    AtomicReference<Short> result = new AtomicReference<>();
    ForkUnderFileSystemUtils.invokeOne(
        new Function<Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Short>>,
            IOException>() {
          @Nullable
          @Override
          public IOException apply(
              Pair<Map.Entry<String, UnderFileSystem>, AtomicReference<Short>> arg) {
            try {
              Map.Entry<String, UnderFileSystem> entry = arg.getKey();
              AtomicReference<Short> result = arg.getValue();
              result.set(entry.getValue().getMode(convert(entry.getKey(), path)));
            } catch (IOException e) {
              return e;
            }
            return null;
          }
        }, mUnderFileSystems.entrySet(), result);
    return result.get();
  }

  @Override
  public void connectFromMaster(final String hostname) throws IOException {
    ForkUnderFileSystemUtils
        .invokeAll(new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
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
    ForkUnderFileSystemUtils
        .invokeAll(new Function<Map.Entry<String, UnderFileSystem>, IOException>() {
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
