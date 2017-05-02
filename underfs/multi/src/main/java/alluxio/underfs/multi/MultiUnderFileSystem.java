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

package alluxio.underfs.multi;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.security.authorization.Mode;
import alluxio.underfs.AtomicFileOutputStream;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemRegistry;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Virtual implementation of {@link UnderFileSystem} which can be used to aggregate multiple UFSes.
 */
@ThreadSafe
public class MultiUnderFileSystem implements UnderFileSystem {
  private final Map<String, UnderFileSystem> mUnderFileSystems = new HashMap<>();

  /**
   * Constructs a new {@link MultiUnderFileSystem}.
   *
   * @param ufsConf the UFS configuration
   */
  MultiUnderFileSystem(UnderFileSystemConfiguration ufsConf) {
    for (Map.Entry<String, String> entry : ufsConf.getUserSpecifiedConf().entrySet()) {
      if (entry.getKey().endsWith("url")) {
        mUnderFileSystems
            .put(entry.getValue(), UnderFileSystemRegistry.create(entry.getValue(), null));
      }
    }
  }

  @Override
  public String getUnderFSType() {
    return "multi";
  }

  @Override
  public void close() throws IOException {
    for (UnderFileSystem ufs : mUnderFileSystems.values()) {
      ufs.close();
    }
  }

  @Override
  public void configureProperties() throws IOException {
    for (UnderFileSystem ufs : mUnderFileSystems.values()) {
      ufs.configureProperties();
    }
  }

  @Override
  public OutputStream create(String path) throws IOException {
    Set<OutputStream> streams = new HashSet<>();
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      streams.add(entry.getValue().create(convert(path, entry.getKey())));
    }
    return new MultiUnderFileOutputStream(streams);
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    Set<OutputStream> streams = new HashSet<>();
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      streams.add(entry.getValue().create(convert(path, entry.getKey())));
    }
    return new MultiUnderFileOutputStream(streams);
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      // TODO(jiri): error checking
      entry.getValue().deleteDirectory(convert(path, entry.getKey()));
    }
    return true;
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      // TODO(jiri): error checking
      entry.getValue().deleteDirectory(convert(path, entry.getKey()), options);
    }
    return true;
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      // TODO(jiri): error checking
      entry.getValue().deleteFile(convert(path, entry.getKey()));
    }
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      if (entry.getValue().exists(convert(path, entry.getKey()))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().getBlockSizeByte(convert(path, entry.getKey()));
  }

  @Override
  public Object getConf() {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().getFileLocations(convert(path, entry.getKey()));
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().getFileLocations(convert(path, entry.getKey()), options);
  }

  @Override
  public long getFileSize(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().getFileSize(convert(path, entry.getKey()));
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().getModificationTimeMs(convert(path, entry.getKey()));
  }

  @Override
  public Map<String, String> getProperties() {
    return Iterables.getFirst(mUnderFileSystems.values(), null).getProperties();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().getSpace(convert(path, entry.getKey()), type);
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().isDirectory(convert(path, entry.getKey()));
  }

  @Override
  public boolean isFile(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().isFile(convert(path, entry.getKey()));
  }

  @Override
  public UnderFileStatus[] listStatus(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().listStatus(convert(path, entry.getKey()));
  }

  @Override
  public UnderFileStatus[] listStatus(String path, ListOptions options) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().listStatus(convert(path, entry.getKey()), options);
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      // TODO(jiri): error-checking
      entry.getValue().mkdirs(convert(path, entry.getKey()));
    }
    return true;
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      // TODO(jiri): error-checking
      entry.getValue().mkdirs(convert(path, entry.getKey()), options);
    }
    return true;
  }

  @Override
  public InputStream open(String path) throws IOException {
    Set<InputStream> streams = new HashSet<>();
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      streams.add(entry.getValue().open(convert(path, entry.getKey())));
    }
    return new MultiUnderFileInputStream(streams);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    Set<InputStream> streams = new HashSet<>();
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      streams.add(entry.getValue().open(convert(path, entry.getKey()), options));
    }
    return new MultiUnderFileInputStream(streams);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      // TODO(jiri): error-checking
      entry.getValue().renameDirectory(convert(src, entry.getKey()), convert(dst, entry.getKey()));
    }
    return true;
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      // TODO(jiri): error-checking
      entry.getValue().renameFile(convert(src, entry.getKey()), convert(dst, entry.getKey()));
    }
    return true;
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setConf(Object conf) {}

  @Override
  public void setProperties(Map<String, String> properties) {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      entry.getValue().setProperties(properties);
    }
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      entry.getValue().setOwner(convert(path, entry.getKey()), user, group);
    }
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    for (Map.Entry<String, UnderFileSystem> entry : mUnderFileSystems.entrySet()) {
      entry.getValue().setMode(convert(path, entry.getKey()), mode);
    }
  }

  @Override
  public String getOwner(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().getOwner(convert(path, entry.getKey()));
  }

  @Override
  public String getGroup(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().getGroup(convert(path, entry.getKey()));
  }

  @Override
  public short getMode(String path) throws IOException {
    Map.Entry<String, UnderFileSystem> entry =
        Iterables.getFirst(mUnderFileSystems.entrySet(), null);
    return entry.getValue().getMode(convert(path, entry.getKey()));
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    // No-op
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    // No-op
  }

  @Override
  public boolean supportsFlush() {
    return true;
  }

  private String convert(String path, String prefix) {
    return prefix + (new AlluxioURI(path)).getPath();
  }
}
