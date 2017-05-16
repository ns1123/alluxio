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
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
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
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.master.privilege.PrivilegeChecker;
import alluxio.master.privilege.PrivilegeMaster;
import alluxio.proto.journal.Journal;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.UfsInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.Privilege;
import alluxio.wire.TtlAction;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.ImmutableSet;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
   * @param journalFactory the journal factory
   */
  PrivilegedFileSystemMaster(BlockMaster blockMaster, PrivilegeMaster privilegeMaster,
      JournalFactory journalFactory) {
    mFileSystemMaster = new DefaultFileSystemMaster(blockMaster, journalFactory);
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
      UnexpectedAlluxioException {
    mPrivilegeChecker.check(Privilege.FREE);
    mFileSystemMaster.free(path, options);
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    if (options.getPinned() != null) {
      mPrivilegeChecker.check(Privilege.PIN);
    }
    if (options.getReplicationMin() != null && options.getReplicationMin() > 0) {
      mPrivilegeChecker.check(Privilege.REPLICATION);
    }
    if (options.getTtl() != null) {
      mPrivilegeChecker.check(Privilege.TTL);
    }
    mFileSystemMaster.setAttribute(path, options);
  }

  @Override
  public StartupConsistencyCheck getStartupConsistencyCheck() {
    return mFileSystemMaster.getStartupConsistencyCheck();
  }

  @Override
  public long getFileId(AlluxioURI path) throws AccessControlException {
    return mFileSystemMaster.getFileId(path);
  }

  @Override
  public FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException {
    return mFileSystemMaster.getFileInfo(fileId);
  }

  @Override
  public FileInfo getFileInfo(AlluxioURI path, GetStatusOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    return mFileSystemMaster.getFileInfo(path, options);
  }

  @Override
  public PersistenceState getPersistenceState(long fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getPersistenceState(fileId);
  }

  @Override
  public List<FileInfo> listStatus(AlluxioURI path, ListStatusOptions listStatusOptions)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException {
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
  public void completeFile(AlluxioURI path, CompleteFileOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, AccessControlException {
    mFileSystemMaster.completeFile(path, options);
  }

  @Override
  public long reinitializeFile(AlluxioURI path, long blockSizeBytes, long ttl, TtlAction ttlAction)
      throws InvalidPathException, FileDoesNotExistException {
    return mFileSystemMaster.reinitializeFile(path, blockSizeBytes, ttl, ttlAction);
  }

  @Override
  public long getNewBlockIdForFile(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    return mFileSystemMaster.getNewBlockIdForFile(path);
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() {
    return mFileSystemMaster.getMountTable();
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
  public List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    return mFileSystemMaster.getFileBlockInfoList(path);
  }

  @Override
  public List<AlluxioURI> getInMemoryFiles() {
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
  public void reportLostFile(long fileId) throws FileDoesNotExistException {
    mFileSystemMaster.reportLostFile(fileId);
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
  public void resetFile(long fileId)
      throws UnexpectedAlluxioException, FileDoesNotExistException, InvalidPathException,
      AccessControlException {
    mFileSystemMaster.resetFile(fileId);
  }

  @Override
  public void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException {
    mFileSystemMaster.scheduleAsyncPersistence(path);
  }

  @Override
  public FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    return mFileSystemMaster.workerHeartbeat(workerId, persistedFiles);
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() {
    return mFileSystemMaster.getWorkerInfoList();
  }
}
