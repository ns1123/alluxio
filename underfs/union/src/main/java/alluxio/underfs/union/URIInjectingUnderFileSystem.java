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
import alluxio.SyncInfo;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsMode;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * An UFS which delegates all calls to another UFS only after properly passing in the proper path
 * which consists of a URI scheme://authority followed by the path to the file.
 */
public class URIInjectingUnderFileSystem implements UnderFileSystem {

  private final UnderFileSystem mDelegate;
  private final String mUriBase;

  URIInjectingUnderFileSystem(UnderFileSystem otherUfs, String baseUri) {
    mDelegate = Preconditions.checkNotNull(otherUfs);
    mUriBase = URI.create(Preconditions.checkNotNull(baseUri)).toString();
  }

  private String convertPath(String inputPath) {
    return PathUtils.concatPath(mUriBase, URI.create(inputPath).getPath());
  }

  @Override
  public String getUnderFSType() {
    return mDelegate.getUnderFSType();
  }

  @Override
  public void cleanup() throws IOException {
    mDelegate.cleanup();
  }

  @Override
  public void close() throws IOException {
    mDelegate.close();
  }

  @Override
  public OutputStream create(final String path) throws IOException {
    return mDelegate.create(convertPath(path));
  }

  @Override
  public OutputStream create(final String path, final CreateOptions options) throws IOException {
    return mDelegate.create(convertPath(path), options);
  }

  @Override
  public OutputStream createNonexistingFile(final String path) throws IOException {
    return mDelegate.createNonexistingFile(convertPath(path));
  }

  @Override
  public OutputStream createNonexistingFile(final String path, CreateOptions options)
      throws IOException {
    return mDelegate.createNonexistingFile(convertPath(path), options);
  }

  @Override
  public boolean deleteDirectory(final String path) throws IOException {
    return mDelegate.deleteDirectory(convertPath(path));
  }

  @Override
  public boolean deleteDirectory(final String path, final DeleteOptions options) throws IOException {
    return mDelegate.deleteDirectory(convertPath(path), options);
  }

  @Override
  public boolean deleteExistingDirectory(final String path) throws IOException {
    return mDelegate.deleteExistingDirectory(convertPath(path));
  }

  @Override
  public boolean deleteExistingDirectory(final String path, final DeleteOptions options) throws IOException {
    return mDelegate.deleteExistingDirectory(convertPath(path), options);
  }

  @Override
  public boolean deleteFile(final String path) throws IOException {
    return mDelegate.deleteFile(convertPath(path));
  }

  @Override
  public boolean deleteExistingFile(final String path) throws IOException {
    return mDelegate.deleteExistingFile(convertPath(path));
  }

  @Override
  public boolean exists(final String path) throws IOException {
    return mDelegate.exists(convertPath(path));
  }

  @Override
  public alluxio.collections.Pair<AccessControlList, DefaultAccessControlList> getAclPair(
      String path) throws IOException {
    return mDelegate.getAclPair(convertPath(path));
  }

  @Override
  public long getBlockSizeByte(final String path) throws IOException {
    return mDelegate.getBlockSizeByte(convertPath(path));
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(final String path) throws IOException {
    return mDelegate.getDirectoryStatus(convertPath(path));
  }

  @Override
  public UfsDirectoryStatus getExistingDirectoryStatus(final String path) throws IOException {
    return mDelegate.getExistingDirectoryStatus(convertPath(path));
  }

  @Override
  public List<String> getFileLocations(final String path) throws IOException {
    return mDelegate.getFileLocations(convertPath(path));
  }

  @Override
  public List<String> getFileLocations(final String path, final FileLocationOptions options)
      throws IOException {
    return mDelegate.getFileLocations(convertPath(path), options);
  }

  @Override
  public UfsFileStatus getFileStatus(final String path) throws IOException {
    return mDelegate.getFileStatus(convertPath(path));
  }

  @Override
  public UfsFileStatus getExistingFileStatus(final String path) throws IOException {
    return mDelegate.getExistingFileStatus(convertPath(path));
  }

  @Override
  public String getFingerprint(String path) {
    return mDelegate.getFingerprint(convertPath(path));
  }

  @Override
  public long getSpace(final String path, final SpaceType type) throws IOException {
    return mDelegate.getSpace(convertPath(path), type);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return mDelegate.getStatus(convertPath(path));
  }

  @Override
  public UfsStatus getExistingStatus(String path) throws IOException {
    return mDelegate.getExistingStatus(convertPath(path));
  }

  @Override
  public boolean isDirectory(final String path) throws IOException {
    return mDelegate.isDirectory(convertPath(path));
  }

  @Override
  public boolean isExistingDirectory(final String path) throws IOException {
    return mDelegate.isExistingDirectory(convertPath(path));
  }

  @Override
  public boolean isFile(final String path) throws IOException {
    return mDelegate.isFile(convertPath(path));
  }

  @Override
  public boolean isObjectStorage() {
    return mDelegate.isObjectStorage();
  }

  @Override
  public boolean isSeekable() {
    return mDelegate.isSeekable();
  }

  @Override
  public UfsStatus[] listStatus(final String path) throws IOException {
    return mDelegate.listStatus(convertPath(path));
  }

  @Override
  public UfsStatus[] listStatus(final String path, final ListOptions options)
      throws IOException {
    return mDelegate.listStatus(convertPath(path), options);
  }

  @Override
  public boolean mkdirs(final String path) throws IOException {
    return mDelegate.mkdirs(convertPath(path));
  }

  @Override
  public boolean mkdirs(final String path, final MkdirsOptions options) throws IOException {
    return mDelegate.mkdirs(convertPath(path), options);
  }

  @Override
  public InputStream open(final String path) throws IOException {
    return mDelegate.open(convertPath(path));
  }

  @Override
  public InputStream open(final String path, final OpenOptions options) throws IOException {
    return mDelegate.open(convertPath(path), options);
  }

  @Override
  public InputStream openExistingFile(final String path) throws IOException {
    return mDelegate.openExistingFile(convertPath(path));
  }

  @Override
  public InputStream openExistingFile(final String path, final OpenOptions options) throws IOException {
    return mDelegate.openExistingFile(convertPath(path), options);
  }

  @Override
  public boolean renameDirectory(final String src, final String dst) throws IOException {
    return mDelegate.renameDirectory(convertPath(src), convertPath(dst));
  }

  @Override
  public boolean renameRenamableDirectory(final String src, final String dst) throws IOException {
    return mDelegate.renameRenamableDirectory(convertPath(src), convertPath(dst));
  }

  @Override
  public boolean renameFile(final String src, final String dst) throws IOException {
    return mDelegate.renameFile(convertPath(src), convertPath(dst));
  }

  @Override
  public boolean renameRenamableFile(final String src, final String dst) throws IOException {
    return mDelegate.renameRenamableFile(convertPath(src), convertPath(dst));
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return mDelegate.resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    mDelegate.setAclEntries(convertPath(path), aclEntries);
  }

  @Override
  public void setOwner(final String path, final String user, final String group)
      throws IOException {
    mDelegate.setOwner(convertPath(path), user, group);
  }

  @Override
  public void setMode(final String path, final short mode) throws IOException {
    mDelegate.setMode(convertPath(path), mode);
  }

  @Override
  public void connectFromMaster(final String hostname) throws IOException {
    mDelegate.connectFromMaster(hostname);
  }

  @Override
  public void connectFromWorker(final String hostname) throws IOException {
    mDelegate.connectFromWorker(hostname);
  }

  @Override
  public boolean supportsFlush() throws IOException {
    return mDelegate.supportsFlush();
  }

  @Override
  public boolean supportsActiveSync() {
    return mDelegate.supportsActiveSync();
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    return mDelegate.startActiveSyncPolling(txId);
  }

  @Override
  public boolean stopActiveSyncPolling() throws IOException {
    return mDelegate.stopActiveSyncPolling();
  }

  @Override
  public SyncInfo getActiveSyncInfo() throws IOException {
    return mDelegate.getActiveSyncInfo();
  }

  @Override
  public void startSync(AlluxioURI uri) throws IOException {
    mDelegate.startSync(new AlluxioURI(convertPath(uri.getPath())));
  }

  @Override
  public void stopSync(AlluxioURI uri) throws IOException {
    mDelegate.stopSync(new AlluxioURI(convertPath(uri.getPath())));
  }

  @Override
  public UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState) {
    return mDelegate.getOperationMode(physicalUfsState);
  }

  @Override
  public List<String> getPhysicalStores() {
    return mDelegate.getPhysicalStores();
  }
}
