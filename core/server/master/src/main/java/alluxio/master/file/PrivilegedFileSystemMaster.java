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
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.grpc.SetAclAction;
import alluxio.master.CoreMasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.contexts.CheckConsistencyContext;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.contexts.ScheduleAsyncPersistenceContext;
import alluxio.master.file.contexts.SetAclContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.contexts.WorkerHeartbeatContext;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.privilege.PrivilegeChecker;
import alluxio.master.privilege.PrivilegeMaster;
import alluxio.metrics.TimeSeries;
import alluxio.proto.journal.Journal;
import alluxio.security.authentication.ClientIpAddressInjector;
import alluxio.security.authorization.AclEntry;
import alluxio.underfs.UfsMode;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.FileSystemCommand;
import alluxio.wire.MountPointInfo;
import alluxio.wire.Privilege;
import alluxio.wire.SyncPointInfo;
import alluxio.wire.UfsInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.ImmutableSet;
import io.grpc.ServerInterceptors;

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
  public void close() throws IOException {
    mFileSystemMaster.close();
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
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(ServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE, new GrpcService(ServerInterceptors
        .intercept(new FileSystemMasterClientServiceHandler(this), new ClientIpAddressInjector())));
    services.put(ServiceType.FILE_SYSTEM_MASTER_JOB_SERVICE,
        new GrpcService(new FileSystemMasterJobServiceHandler(this)));
    services.put(ServiceType.FILE_SYSTEM_MASTER_WORKER_SERVICE,
        new GrpcService(new FileSystemMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    return mFileSystemMaster.processJournalEntry(entry);
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
  public long createDirectory(AlluxioURI path, CreateDirectoryContext context)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException {
    if (context.getTtl() != alluxio.Constants.NO_TTL) {
      mPrivilegeChecker.check(Privilege.TTL);
    }
    return mFileSystemMaster.createDirectory(path, context);
  }

  @Override
  public FileInfo createFile(AlluxioURI path, CreateFileContext context)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
      BlockInfoException, IOException, FileDoesNotExistException {
    if (context.getOptions().getReplicationMin() > 0) {
      mPrivilegeChecker.check(Privilege.REPLICATION);
    }
    if (context.getTtl() != Constants.NO_TTL) {
      mPrivilegeChecker.check(Privilege.TTL);
    }
    return mFileSystemMaster.createFile(path, context);
  }

  @Override
  public void free(AlluxioURI path, FreeContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException, UnavailableException, IOException {
    try {
      mPrivilegeChecker.check(Privilege.FREE);
    } catch (PermissionDeniedException | UnauthenticatedException e) {
      throw new AccessControlException(e.getMessage(), e);
    }
    mFileSystemMaster.free(path, context);
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributeContext context)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException, IOException {
    try {
      if (context.getOptions().hasPinned()) {
        mPrivilegeChecker.check(Privilege.PIN);
      }
      if (context.getOptions().hasReplicationMin()
          && context.getOptions().getReplicationMin() > 0) {
        mPrivilegeChecker.check(Privilege.REPLICATION);
      }
      if (context.getOptions().getCommonOptions().hasTtl()) {
        mPrivilegeChecker.check(Privilege.TTL);
      }
    } catch (PermissionDeniedException | UnauthenticatedException e) {
      throw new AccessControlException(e.getMessage(), e.getCause());
    }
    mFileSystemMaster.setAttribute(path, context);
  }

  @Override
  public void cleanupUfs() {
    mFileSystemMaster.cleanupUfs();
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
  public FileInfo getFileInfo(AlluxioURI path, GetStatusContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnavailableException, IOException {
    return mFileSystemMaster.getFileInfo(path, context);
  }

  @Override
  public PersistenceState getPersistenceState(long fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getPersistenceState(fileId);
  }

  @Override
  public List<FileInfo> listStatus(AlluxioURI path, ListStatusContext context)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException,
      UnavailableException, IOException {
    return mFileSystemMaster.listStatus(path, context);
  }

  // ALLUXIO CS ADD
  @Override
  public void scan(java.util.function.BiConsumer<String, alluxio.master.file.meta.InodeView> fn) {
    mFileSystemMaster.scan(fn);
  }

  @Override
  public void exec(long inodeId, alluxio.master.file.meta.InodeTree.LockPattern lockPattern,
      alluxio.function.ThrowableConsumer<ExecContext> fn) throws Exception {
    mFileSystemMaster.exec(inodeId, lockPattern, fn);
  }
  // ALLUXIO CS END
  @Override
  public FileSystemMasterView getFileSystemMasterView() {
    return mFileSystemMaster.getFileSystemMasterView();
  }

  @Override
  public List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyContext context)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException, IOException {
    return mFileSystemMaster.checkConsistency(path, context);
  }

  @Override
  public void completeFile(AlluxioURI path, CompleteFileContext context) throws BlockInfoException,
      FileDoesNotExistException, InvalidPathException, InvalidFileSizeException,
      FileAlreadyCompletedException, AccessControlException, UnavailableException {
    mFileSystemMaster.completeFile(path, context);
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
  public MountPointInfo getDisplayMountPointInfo(AlluxioURI path) throws InvalidPathException {
    return mFileSystemMaster.getDisplayMountPointInfo(path);
  }

  @Override
  public long getInodeCount() {
    return mFileSystemMaster.getInodeCount();
  }

  @Override
  public int getNumberOfPinnedFiles() {
    return mFileSystemMaster.getNumberOfPinnedFiles();
  }

  @Override
  public void delete(AlluxioURI path, DeleteContext context) throws IOException,
      FileDoesNotExistException, DirectoryNotEmptyException, InvalidPathException,
      AccessControlException {
    mFileSystemMaster.delete(path, context);
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
  public void rename(AlluxioURI srcPath, AlluxioURI dstPath, RenameContext context)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    mFileSystemMaster.rename(srcPath, dstPath, context);
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
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountContext context)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    mFileSystemMaster.mount(alluxioPath, ufsPath, context);
  }

  @Override
  public void unmount(AlluxioURI alluxioPath)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException {
    mFileSystemMaster.unmount(alluxioPath);
  }

  @Override
  public void updateMount(AlluxioURI alluxioPath, MountContext context)
      throws FileAlreadyExistsException, FileDoesNotExistException,
      InvalidPathException, IOException, AccessControlException {
    mFileSystemMaster.updateMount(alluxioPath, context);
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action,
      List<AclEntry> entries, SetAclContext context)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException, IOException {
    mFileSystemMaster.setAcl(path, action, entries, context);
  }

  @Override
  public void scheduleAsyncPersistence(AlluxioURI path, ScheduleAsyncPersistenceContext context)
      throws AlluxioException, UnavailableException {
    mFileSystemMaster.scheduleAsyncPersistence(path, context);
  }

  @Override
  public FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles,
      WorkerHeartbeatContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException, IOException {
    return mFileSystemMaster.workerHeartbeat(workerId, persistedFiles, context);
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() throws UnavailableException {
    return mFileSystemMaster.getWorkerInfoList();
  }

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
  public List<TimeSeries> getTimeSeries() {
    return mFileSystemMaster.getTimeSeries();
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

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.FILE_SYSTEM_MASTER;
  }
}
