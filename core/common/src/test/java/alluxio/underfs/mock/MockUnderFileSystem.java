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

package alluxio.underfs.mock;

import alluxio.AlluxioURI;
import alluxio.SyncInfo;
import alluxio.collections.Pair;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Mock implementation of {@link alluxio.underfs.UnderFileSystem} used for testing.
 */
public class MockUnderFileSystem implements UnderFileSystem {
  private final UnderFileSystemConfiguration mUfsConf;

  /**
   * Creates a new instance of {@link MockUnderFileSystem}.
   *
   * @param ufsConf the under file system configuration
   */
  public MockUnderFileSystem(UnderFileSystemConfiguration ufsConf) {
    mUfsConf = ufsConf;
  }

  @Override
  public void cleanup() throws IOException {}

  @Override
  public void close() throws IOException {}

  @Override
  public void connectFromMaster(String hostname) throws IOException {}

  @Override
  public void connectFromWorker(String hostname) throws IOException {}

  @Override
  public OutputStream create(String path) throws IOException {
    return null;
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    return null;
  }

  @Override
  public OutputStream createNonexistingFile(String path) throws IOException {
    return null;
  }

  @Override
  public OutputStream createNonexistingFile(String path, CreateOptions options) throws IOException {
    return null;
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    return false;
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    return false;
  }

  @Override
  public boolean deleteExistingDirectory(String path) throws IOException {
    return false;
  }

  @Override
  public boolean deleteExistingDirectory(String path, DeleteOptions options) throws IOException {
    return false;
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return false;
  }

  @Override
  public boolean deleteExistingFile(String path) throws IOException {
    return false;
  }

  @Override
  public boolean exists(String path) throws IOException {
    return false;
  }

  @Override
  public Pair<AccessControlList, DefaultAccessControlList> getAclPair(String path)
      throws IOException {
    return new Pair<>(null, null);
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return -1;
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    return null;
  }

  @Override
  public UfsDirectoryStatus getExistingDirectoryStatus(String path) throws IOException {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options) throws IOException {
    return null;
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    return null;
  }

  @Override
  public UfsFileStatus getExistingFileStatus(String path) throws IOException {
    return null;
  }

  @Override
  public String getFingerprint(String path) {
    return null;
  }

  public String getProperty(String key) {
    return mUfsConf.getMountSpecificConf().get(key);
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return null;
  }

  @Override
  public UfsStatus getExistingStatus(String path) throws IOException {
    return null;
  }

  @Override
  public String getUnderFSType() {
    return "mock";
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return false;
  }

  @Override
  public boolean isExistingDirectory(String path) throws IOException {
    return false;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return false;
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
  public UfsStatus[] listStatus(String path) throws IOException {
    return null;
  }

  @Override
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    return null;
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    return false;
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    return false;
  }

  @Override
  public InputStream open(String path) throws IOException {
    return null;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    return null;
  }

  @Override
  public InputStream openExistingFile(String path, OpenOptions options) throws IOException {
    return null;
  }

  @Override
  public InputStream openExistingFile(String path) throws IOException {
    return null;
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    return false;
  }

  @Override
  public boolean renameRenamableDirectory(String src, String dst) throws IOException {
    return false;
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    return false;
  }

  @Override
  public boolean renameRenamableFile(String src, String dst) throws IOException {
    return false;
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return null;
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException{
  }

  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {}

  @Override
  public boolean supportsFlush() {
    return false;
  }

  @Override
  public boolean supportsActiveSync() {
    return false;
  }

  @Override
  public SyncInfo getActiveSyncInfo() {
    return SyncInfo.emptyInfo();
  }

  @Override
  public void startSync(AlluxioURI uri) {
  }

  @Override
  public void stopSync(AlluxioURI uri) {
  }

  @Override
  public boolean startActiveSyncPolling(long txId) {
    return false;
  }

  @Override
  public boolean stopActiveSyncPolling() {
    return false;
  }

  @Override
  public UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState) {
    return null;
  }

  @Override
  public List<String> getPhysicalStores() {
    return null;
  }
}
