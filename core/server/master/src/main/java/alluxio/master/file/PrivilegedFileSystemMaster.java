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
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.master.privilege.PrivilegeChecker;
import alluxio.master.privilege.PrivilegeMaster;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.FileSystemMasterWorkerService;

import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * File system master that checks privileges.
 */
public class PrivilegedFileSystemMaster extends DefaultFileSystemMaster {
  /** This checks user privileges on privileged operations. */
  private final PrivilegeChecker mPrivilegeChecker;

  PrivilegedFileSystemMaster(BlockMaster blockMaster, PrivilegeMaster privilegeMaster,
      JournalFactory journalFactory) {
    super(blockMaster, journalFactory);
    mPrivilegeChecker = new PrivilegeChecker(privilegeMaster);
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
  public long createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException {
    if (options.getTtl() != alluxio.Constants.NO_TTL) {
      mPrivilegeChecker.check(alluxio.wire.Privilege.TTL);
    }
    return super.createDirectory(path, options);
  }

  @Override
  public long createFile(AlluxioURI path, CreateFileOptions options)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
      BlockInfoException, IOException, FileDoesNotExistException {
    if (options.getReplicationMin() > 0) {
      mPrivilegeChecker.check(alluxio.wire.Privilege.REPLICATION);
    }
    if (options.getTtl() != Constants.NO_TTL) {
      mPrivilegeChecker.check(alluxio.wire.Privilege.TTL);
    }
    return super.createFile(path, options);
  }

  @Override
  public void free(AlluxioURI path, FreeOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException {
    mPrivilegeChecker.check(alluxio.wire.Privilege.FREE);
    super.free(path, options);
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    if (options.getPinned() != null) {
      mPrivilegeChecker.check(alluxio.wire.Privilege.PIN);
    }
    if (options.getReplicationMin() != null && options.getReplicationMin() > 0) {
      mPrivilegeChecker.check(alluxio.wire.Privilege.REPLICATION);
    }
    if (options.getTtl() != null || options.getTtlAction() != null) {
      mPrivilegeChecker.check(alluxio.wire.Privilege.TTL);
    }
    super.setAttribute(path, options);
  }
}
