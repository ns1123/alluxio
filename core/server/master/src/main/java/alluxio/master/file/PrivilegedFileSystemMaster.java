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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.Server;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.CoreMasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.GetStatusOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAclOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.file.options.WorkerHeartbeatOptions;
import alluxio.master.journal.JournalContext;
import alluxio.master.privilege.PrivilegeChecker;
import alluxio.master.privilege.PrivilegeMaster;
import alluxio.proto.journal.Journal;
import alluxio.security.authorization.AclEntry;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.UfsInfo;
import alluxio.underfs.UnderFileSystem.UfsMode;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.Privilege;
import alluxio.wire.SetAclAction;
import alluxio.wire.SyncPointInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.ImmutableSet;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * File system master that checks privileges.
 */
public class PrivilegedFileSystemMaster implements FileSystemMaster {
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(BlockMaster.class, PrivilegeMaster.class);

  /** This checks user privileges on privileged operations. */
  private final PrivilegeChecker mPrivilegeChecker;

  /** The underlying file system master. */
  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link PrivilegedFileSystemMaster}.
   *
   * @param blockMaster the block master
   * @param privilegeMaster the privilege master
   * @param masterContext the context for Alluxio master
   */
  PrivilegedFileSystemMaster(BlockMaster blockMaster, PrivilegeMaster privilegeMaster,
      CoreMasterContext masterContext) {
    mFileSystemMaster = new DefaultFileSystemMaster(blockMaster, masterContext);
    mPrivilegeChecker = new PrivilegeChecker(privilegeMaster);
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    mFileSystemMaster.start(isLeader);
  }

  @Override
  public void stop() throws IOException {
    mFileSystemMaster.stop();
  }

  @Override
  public String getName() {
    return Constants.FILE_SYSTEM_MASTER_NAME;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME,
        new FileSystemMasterClientService.Processor<>(
            new FileSystemMasterClientServiceHandler(this)));
    services.put(Constants.FILE_SYSTEM_MASTER_JOB_SERVICE_NAME,
        new alluxio.thrift.FileSystemMasterJobService.Processor<>(
            new FileSystemMasterJobServiceHandler(this)));
    services.put(Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME,
        new FileSystemMasterWorkerService.Processor<>(
            new FileSystemMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public void processJournalEntry(Journal.JournalEntry entry) throws IOException {
    mFileSystemMaster.processJournalEntry(entry);
  }

  @Override
  public void resetState() {
    mFileSystemMaster.resetState();
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    return mFileSystemMaster.getJournalEntryIterator();
  }

  @Override
  public long createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException {
    if (options.getTtl() != alluxio.Constants.NO_TTL) {
      mPrivilegeChecker.check(Privilege.TTL);
    }
    return mFileSystemMaster.createDirectory(path, options);
  }

  @Override
  public long createFile(AlluxioURI path, CreateFileOptions options)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
      BlockInfoException, IOException, FileDoesNotExistException {
    if (options.getReplicationMin() > 0) {
      mPrivilegeChecker.check(Privilege.REPLICATION);
    }
    if (options.getTtl() != Constants.NO_TTL) {
      mPrivilegeChecker.check(Privilege.TTL);
    }
    return mFileSystemMaster.createFile(path, options);
  }

  @Override
  public void free(AlluxioURI path, FreeOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException, UnavailableException, IOException {
    try {
      mPrivilegeChecker.check(Privilege.FREE);
    } catch (PermissionDeniedException | UnauthenticatedException e) {
      throw new AccessControlException(e.getMessage(), e);
    }
    mFileSystemMaster.free(path, options);
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException, IOException {
    try {
      if (options.getPinned() != null) {
        mPrivilegeChecker.check(Privilege.PIN);
      }
      if (options.getReplicationMin() != null && options.getReplicationMin() > 0) {
        mPrivilegeChecker.check(Privilege.REPLICATION);
      }
      if (options.getTtl() != null) {
        mPrivilegeChecker.check(Privilege.TTL);
      }
    } catch (PermissionDeniedException | UnauthenticatedException e) {
      throw new AccessControlException(e.getMessage(), e.getCause());
    }
    mFileSystemMaster.setAttribute(path, options);
  }

  @Override
  public void cleanupUfs() {
    mFileSystemMaster.cleanupUfs();
  }

  @Override
  public StartupConsistencyCheck getStartupConsistencyCheck() {
    return mFileSystemMaster.getStartupConsistencyCheck();
  }

  @Override
  public long getFileId(AlluxioURI path) throws AccessControlException, UnavailableException {
    return mFileSystemMaster.getFileId(path);
  }

  @Override
  public FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException, UnavailableException {
    return mFileSystemMaster.getFileInfo(fileId);
  }

  @Override
  public FileInfo getFileInfo(AlluxioURI path, GetStatusOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnavailableException, IOException {
    return mFileSystemMaster.getFileInfo(path, options);
  }

  @Override
  public PersistenceState getPersistenceState(long fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getPersistenceState(fileId);
  }

  @Override
  public List<FileInfo> listStatus(AlluxioURI path, ListStatusOptions listStatusOptions)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException,
      UnavailableException, IOException {
    return mFileSystemMaster.listStatus(path, listStatusOptions);
  }

  @Override
  public FileSystemMasterView getFileSystemMasterView() {
    return mFileSystemMaster.getFileSystemMasterView();
  }

  @Override
  public List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyOptions options)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException, IOException {
    return mFileSystemMaster.checkConsistency(path, options);
  }

  @Override
  public void completeFile(AlluxioURI path, CompleteFileOptions options) throws BlockInfoException,
      FileDoesNotExistException, InvalidPathException, InvalidFileSizeException,
      FileAlreadyCompletedException, AccessControlException, UnavailableException {
    mFileSystemMaster.completeFile(path, options);
  }

  @Override
  public long getNewBlockIdForFile(AlluxioURI path) throws FileDoesNotExistException,
      InvalidPathException, AccessControlException, UnavailableException {
    return mFileSystemMaster.getNewBlockIdForFile(path);
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() {
    return mFileSystemMaster.getMountTable();
  }

  @Override
  public MountPointInfo getMountPointInfo(AlluxioURI path) throws InvalidPathException {
    return mFileSystemMaster.getMountPointInfo(path);
  }

  @Override
  public int getNumberOfPaths() {
    return mFileSystemMaster.getNumberOfPaths();
  }

  @Override
  public int getNumberOfPinnedFiles() {
    return mFileSystemMaster.getNumberOfPinnedFiles();
  }

  @Override
  public void delete(AlluxioURI path, DeleteOptions options) throws IOException,
      FileDoesNotExistException, DirectoryNotEmptyException, InvalidPathException,
      AccessControlException {
    mFileSystemMaster.delete(path, options);
  }

  @Override
  public List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path) throws FileDoesNotExistException,
      InvalidPathException, AccessControlException, UnavailableException {
    return mFileSystemMaster.getFileBlockInfoList(path);
  }

  @Override
  public List<AlluxioURI> getInAlluxioFiles() throws UnavailableException {
    return mFileSystemMaster.getInMemoryFiles();
  }

  @Override
  public List<AlluxioURI> getInMemoryFiles() throws UnavailableException {
    return mFileSystemMaster.getInMemoryFiles();
  }

  @Override
  public void rename(AlluxioURI srcPath, AlluxioURI dstPath, RenameOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    mFileSystemMaster.rename(srcPath, dstPath, options);
  }

  @Override
  public AlluxioURI getPath(long fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getPath(fileId);
  }

  @Override
  public Set<Long> getPinIdList() {
    return mFileSystemMaster.getPinIdList();
  }

  @Override
  public String getUfsAddress() {
    return mFileSystemMaster.getUfsAddress();
  }

  @Override
  public UfsInfo getUfsInfo(long mountId) {
    return mFileSystemMaster.getUfsInfo(mountId);
  }

  @Override
  public List<String> getWhiteList() {
    return mFileSystemMaster.getWhiteList();
  }

  @Override
  public List<Long> getLostFiles() {
    return mFileSystemMaster.getLostFiles();
  }

  @Override
  public long loadMetadata(AlluxioURI path, LoadMetadataOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, IOException, AccessControlException {
    return mFileSystemMaster.loadMetadata(path, options);
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    mFileSystemMaster.mount(alluxioPath, ufsPath, options);
  }

  @Override
  public void unmount(AlluxioURI alluxioPath)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException {
    mFileSystemMaster.unmount(alluxioPath);
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action,
      List<AclEntry> entries, SetAclOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException, IOException {
    mFileSystemMaster.setAcl(path, action, entries, options);
  }

  @Override
  public void scheduleAsyncPersistence(AlluxioURI path)
      throws AlluxioException, UnavailableException {
    mFileSystemMaster.scheduleAsyncPersistence(path);
  }

  @Override
  public FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles,
      WorkerHeartbeatOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException, IOException {
    return mFileSystemMaster.workerHeartbeat(workerId, persistedFiles, options);
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() throws UnavailableException {
    return mFileSystemMaster.getWorkerInfoList();
  }

  // ALLUXIO CS ADD
  @Override
  public
      alluxio.security.authentication.Token<alluxio.security.authentication.DelegationTokenIdentifier>
      getDelegationToken(String renewer)
      throws AccessControlException, UnavailableException {
    return mFileSystemMaster.getDelegationToken(renewer);
  }

  @Override
  public long renewDelegationToken(
      alluxio.security.authentication.Token<alluxio.security.authentication.DelegationTokenIdentifier>
          delegationToken)
      throws AccessControlException, UnavailableException {
    return mFileSystemMaster.renewDelegationToken(delegationToken);
  }

  @Override
  public void cancelDelegationToken(
      alluxio.security.authentication.Token<alluxio.security.authentication.DelegationTokenIdentifier>
          delegationToken)
      throws AccessControlException, UnavailableException {
    mFileSystemMaster.cancelDelegationToken(delegationToken);
  }

  // ALLUXIO CS END
  @Override
  public List<SyncPointInfo> getSyncPathList() throws UnavailableException, AccessControlException {
    return mFileSystemMaster.getSyncPathList();
  }

  @Override
  public void startSync(AlluxioURI alluxioURI) throws IOException, InvalidPathException,
      AccessControlException, ConnectionFailedException {
    mFileSystemMaster.startSync(alluxioURI);
  }

  @Override
  public void stopSync(AlluxioURI alluxioURI) throws IOException, InvalidPathException,
      AccessControlException {
    mFileSystemMaster.stopSync(alluxioURI);
  }

  @Override
  public void activeSyncMetadata(AlluxioURI path, Collection<AlluxioURI> changedFiles,
      ExecutorService executorService) throws IOException {
    mFileSystemMaster.activeSyncMetadata(path, changedFiles, executorService);
  }

  @Override
  public boolean recordActiveSyncTxid(long txId, long mountId) {
    return mFileSystemMaster.recordActiveSyncTxid(txId, mountId);
  }

  @Override
  public void updateUfsMode(AlluxioURI ufsPath, UfsMode ufsMode) throws InvalidPathException,
      InvalidArgumentException, UnavailableException, AccessControlException {
    mFileSystemMaster.updateUfsMode(ufsPath, ufsMode);
  }

  @Override
  public void validateInodeBlocks(boolean repair) throws UnavailableException {
    mFileSystemMaster.validateInodeBlocks(repair);
  }

  @Override
  public JournalContext createJournalContext() throws UnavailableException {
    return mFileSystemMaster.createJournalContext();
  }
}
