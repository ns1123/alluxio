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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.master.MasterInquireClient;
import alluxio.security.authorization.AclEntry;
import alluxio.uri.Authority;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
* Default implementation of the {@link FileSystem} interface. Developers can extend this class
* instead of implementing the interface. This implementation reads and writes data through
* {@link FileInStream} and {@link FileOutStream}. This class is thread safe.
*/
@PublicApi
@ThreadSafe
public class BaseFileSystem implements FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFileSystem.class);

  protected final FileSystemContext mFileSystemContext;

  /**
   * @param context file system context
   * @return a {@link BaseFileSystem}
   */
  public static BaseFileSystem get(FileSystemContext context) {
    return new BaseFileSystem(context);
  }

  /**
   * Constructs a new base file system.
   *
   * @param context file system context
   */
  protected BaseFileSystem(FileSystemContext context) {
    mFileSystemContext = context;
  }

  @Override
  public void createDirectory(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    createDirectory(path, CreateDirectoryPOptions.getDefaultInstance());
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.createDirectory(path, options);
      LOG.debug("Created directory {}, options: {}", path.getPath(), options);
    } catch (AlreadyExistsException e) {
      throw new FileAlreadyExistsException(e.getMessage());
    } catch (InvalidArgumentException e) {
      throw new InvalidPathException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileOutStream createFile(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    return createFile(path, CreateFilePOptions.getDefaultInstance());
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    URIStatus status;
    try {
      // ALLUXIO CS ADD
      long requestedBlockSizeBytes = options.getBlockSizeBytes();
      if (!options.hasBlockSizeBytes()) {
        requestedBlockSizeBytes =
            alluxio.Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
      }
      if (alluxio.Configuration.getBoolean(alluxio.PropertyKey.SECURITY_ENCRYPTION_ENABLED)) {
        long physicalBlockSize = alluxio.client.LayoutUtils.toPhysicalBlockLength(
            alluxio.client.EncryptionMetaFactory.createLayout(), requestedBlockSizeBytes);
        options = options.toBuilder().setBlockSizeBytes(physicalBlockSize).build();
      }
      // ALLUXIO CS END
      masterClient.createFile(path, options);
      // Do not sync before this getStatus, since the UFS file is expected to not exist.
      GetStatusPOptions gsOptions =
          GetStatusPOptions.newBuilder()
                  .setLoadMetadataType(LoadMetadataPType.NEVER)
                  // ALLUXIO CS ADD
                  .setAccessMode(alluxio.security.authorization.Mode.Bits.WRITE.toProto())
                  // ALLUXIO CS END
                  .build();
      status = masterClient.getStatus(path, gsOptions);
      LOG.debug("Created file {}, options: {}", path.getPath(), options);
    } catch (AlreadyExistsException e) {
      throw new FileAlreadyExistsException(e.getMessage());
    } catch (InvalidArgumentException e) {
      throw new InvalidPathException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }

    OutStreamOptions outStreamOptions = new OutStreamOptions(options);
    outStreamOptions.setUfsPath(status.getUfsPath());
    outStreamOptions.setMountId(status.getMountId());
    // ALLUXIO CS ADD
    if (status.getCapability() != null) {
      outStreamOptions.setCapabilityFetcher(
          new alluxio.client.security.CapabilityFetcher(mFileSystemContext, status.getPath(),
              status.getCapability()));
    }
    outStreamOptions.setEncrypted(status.isEncrypted());
    if (status.isEncrypted()) {
      // Encryption meta is always initialized during file creation and write.
      alluxio.proto.security.EncryptionProto.Meta meta =
          alluxio.client.EncryptionMetaFactory.create(status.getFileId(),
              status.getFileId() /* encryption id */, options.getBlockSizeBytes());
      outStreamOptions.setEncryptionMeta(meta);
      mFileSystemContext.putEncryptionMeta(status.getFileId(), meta);
    }
    // ALLUXIO CS END
    outStreamOptions.setAcl(status.getAcl());
    try {
      // ALLUXIO CS ADD
      if (outStreamOptions.isEncrypted()) {
        return new CryptoFileOutStream(path, outStreamOptions, mFileSystemContext);
      }
      // ALLUXIO CS END
      return new FileOutStream(path, outStreamOptions, mFileSystemContext);
    } catch (Exception e) {
      delete(path);
      throw e;
    }
  }

  @Override
  public void delete(AlluxioURI path)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    delete(path, DeletePOptions.getDefaultInstance());
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.delete(path, options);
      LOG.debug("Deleted {}, options: {}", path.getPath(), options);
    } catch (FailedPreconditionException e) {
      // A little sketchy, but this should be the only case that throws FailedPrecondition.
      throw new DirectoryNotEmptyException(e.getMessage());
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean exists(AlluxioURI path)
      throws InvalidPathException, IOException, AlluxioException {
    return exists(path, ExistsPOptions.getDefaultInstance());
  }

  @Override
  public boolean exists(AlluxioURI path, ExistsPOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Make this more efficient
      masterClient.getStatus(path, GrpcUtils.toGetStatusOptions(options));
      return true;
    } catch (NotFoundException e) {
      return false;
    } catch (InvalidArgumentException e) {
      // The server will throw this when a prefix of the path is a file.
      // TODO(andrew): Change the server so that a prefix being a file means the path does not exist
      return false;
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void free(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    free(path, FreePOptions.getDefaultInstance());
  }

  @Override
  public void free(AlluxioURI path, FreePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.free(path, options);
      LOG.debug("Freed {}, options: {}", path.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  // ALLUXIO CS ADD
  @Override
  public
      alluxio.security.authentication.Token<alluxio.security.authentication.DelegationTokenIdentifier>
      getDelegationToken(String renewer)
      throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      alluxio.security.authentication.Token<alluxio.security.authentication.DelegationTokenIdentifier> token =
          masterClient.getDelegationToken(renewer);
      LOG.debug("Got delegation token {}, renewer: {}", token, renewer);
      return token;
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public long renewDelegationToken(
      alluxio.security.authentication.Token<alluxio.security.authentication.DelegationTokenIdentifier> token)
      throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      long expirationTime =
          masterClient.renewDelegationToken(token);
      LOG.debug("Renew delegation token {}, new expiration time: {}", token, expirationTime);
      return expirationTime;
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void cancelDelegationToken(
      alluxio.security.authentication.Token<alluxio.security.authentication.DelegationTokenIdentifier> token)
      throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.cancelDelegationToken(token);
      LOG.debug("Cancel delegation token {}", token);
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  // ALLUXIO CS END
  @Override
  public URIStatus getStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return getStatus(path, GetStatusPOptions.getDefaultInstance());
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // ALLUXIO CS REPLACE
      // return masterClient.getStatus(path, options);
      // ALLUXIO CS WITH
      URIStatus physicalStatus = getStatusInternal(masterClient, path, options);
      if (physicalStatus.isEncrypted()) {
        alluxio.proto.security.EncryptionProto.Meta meta =
            mFileSystemContext.getEncryptionMeta(physicalStatus.getFileId());
        return new URIStatus(
            alluxio.client.LayoutUtils.convertFileInfoToLogical(physicalStatus.toFileInfo(), meta));
      }
      return physicalStatus;
      // ALLUXIO CS END
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }
  // ALLUXIO CS ADD

  private URIStatus getStatusInternal(
      FileSystemMasterClient masterClient, AlluxioURI path, GetStatusPOptions options)
      throws IOException {
    URIStatus status = masterClient.getStatus(path, options);
    if (!status.isFolder() && status.isEncrypted()) {
      getEncryptionMeta(status);
    }
    return status;
  }
  // ALLUXIO CS END

  @Override
  public List<URIStatus> listStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return listStatus(path, ListStatusPOptions.getDefaultInstance());
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    // TODO(calvin): Fix the exception handling in the master
    try {
      // ALLUXIO CS REPLACE
      // return masterClient.listStatus(path, options);
      // ALLUXIO CS WITH
      return listStatusInternal(masterClient, path, options);
      // ALLUXIO CS END
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }
  // ALLUXIO CS ADD

  private List<URIStatus> listStatusInternal(
      FileSystemMasterClient masterClient, AlluxioURI path, ListStatusPOptions options)
      throws IOException {
    List<URIStatus> statuses = masterClient.listStatus(path, options);
    List<URIStatus> retval = new java.util.ArrayList<>();
    for (URIStatus status : statuses) {
      if (status.isEncrypted()) {
        if (!status.isFolder()) {
          try {
            alluxio.proto.security.EncryptionProto.Meta meta = getEncryptionMeta(status);
            alluxio.wire.FileInfo fileInfo =
                alluxio.client.LayoutUtils.convertFileInfoToLogical(status.toFileInfo(), meta);
            status = new URIStatus(fileInfo);
          } catch (IOException e) {
            LOG.warn("Failed to decode or convert to logical file info for file {}, showing "
                + "physical status.", status.getPath());
          }
        }
        retval.add(status);
      } else {
        // This is assuming encryption is a cluster wide knob. Need to update this once we support
        // mixed encrypted/unencrypted files in one directory.
        return statuses;
      }
    }
    return retval;
  }

  private alluxio.proto.security.EncryptionProto.Meta getEncryptionMeta(URIStatus status)
      throws IOException {
    long fileId = status.getFileId();
    // Criteria to find the encryption metadata:
    // 1. Lookup in the client cache
    alluxio.proto.security.EncryptionProto.Meta meta = mFileSystemContext.getEncryptionMeta(fileId);
    if (meta == null) {
      // 2. Read from file footer with unencrypted fileInStream. It will locate to the
      // UFS physical offset if the footer is not in Alluxio memory.
      meta = getEncryptionMetaFromFooter(status);
      mFileSystemContext.putEncryptionMeta(fileId, meta);
    }
    return meta;
  }

  private alluxio.proto.security.EncryptionProto.Meta getEncryptionMetaFromFooter(URIStatus status)
      throws IOException {
    long fileId = status.getFileId();
    InStreamOptions inStreamOptions = new InStreamOptions(status).setEncrypted(false);
    if (status.getCapability() != null) {
      inStreamOptions.setCapabilityFetcher(
          new alluxio.client.security.CapabilityFetcher(mFileSystemContext, status.getPath(),
              status.getCapability()));
    }
    final int footerMaxSize = alluxio.client.LayoutUtils.getFooterMaxSize();
    byte[] footerBytes = new byte[footerMaxSize];
    try (
        FileInStream fileInStream = new FileInStream(status, inStreamOptions, mFileSystemContext)) {
      if (status.getLength() > footerMaxSize) {
        fileInStream.seek(status.getLength() - footerMaxSize);
      }
      fileInStream.read(footerBytes, 0, footerMaxSize);
    }
    alluxio.proto.layout.FileFooter.FileMetadata fileMetadata =
        alluxio.client.LayoutUtils.decodeFooter(footerBytes);
    alluxio.proto.security.EncryptionProto.CryptoKey cryptoKey;
    try {
      cryptoKey = alluxio.client.security.CryptoUtils.getCryptoKey(
          alluxio.Configuration.get(alluxio.PropertyKey.SECURITY_KMS_ENDPOINT),
          false, String.valueOf(fileMetadata.getEncryptionId()));
    } catch (IOException e) {
      // Allow null crypto key for getStatus and listStatus because one user might not have
      // access to the crypto keys for the files being listed.
      LOG.debug("Can not get the crypto key for encryption id: {}",
          fileMetadata.getEncryptionId());
      cryptoKey = null;
    }
    return alluxio.client.LayoutUtils.fromFooterMetadata(fileId, fileMetadata, cryptoKey);
  }
  // ALLUXIO CS END

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  @Override
  public void loadMetadata(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    loadMetadata(path, LoadMetadataPOptions.getDefaultInstance());
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  @Override
  public void loadMetadata(AlluxioURI path, LoadMetadataPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
//    checkUri(path);
//    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
//    try {
//      masterClient.loadMetadata(path, options);
//      LOG.debug("Loaded metadata {}, options: {}", path.getPath(), options);
//    } catch (NotFoundException e) {
//      throw new FileDoesNotExistException(e.getMessage());
//    } catch (UnavailableException e) {
//      throw e;
//    } catch (AlluxioStatusException e) {
//      throw e.toAlluxioException();
//    } finally {
//      mFileSystemContext.releaseMasterClient(masterClient);
//    }
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath)
      throws IOException, AlluxioException {
    mount(alluxioPath, ufsPath, MountPOptions.getDefaultInstance());
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
      throws IOException, AlluxioException {
    checkUri(alluxioPath);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Make this fail on the master side
      masterClient.mount(alluxioPath, ufsPath, options);
      LOG.info("Mount " + ufsPath.toString() + " to " + alluxioPath.getPath());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      return masterClient.getMountTable();
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      return masterClient.getSyncPathList();
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileInStream openFile(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return openFile(path, OpenFilePOptions.getDefaultInstance());
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    // ALLUXIO CS REPLACE
    // URIStatus status = getStatus(path);
    // ALLUXIO CS WITH
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    URIStatus status;
    try {
      status = getStatusInternal(masterClient, path, GetStatusPOptions.getDefaultInstance());
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
    // ALLUXIO CS END
    if (status.isFolder()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.CANNOT_READ_DIRECTORY.getMessage(status.getName()));
    }
    InStreamOptions inStreamOptions = new InStreamOptions(status, options);
    // ALLUXIO CS ADD
    if (status.getCapability() != null) {
      inStreamOptions.setCapabilityFetcher(
          new alluxio.client.security.CapabilityFetcher(mFileSystemContext, status.getPath(),
              status.getCapability()));
    }
    if (!options.getSkipTransformation()) {
      inStreamOptions.setEncrypted(status.isEncrypted());
      if (status.isEncrypted()) {
        alluxio.proto.security.EncryptionProto.Meta meta =
            mFileSystemContext.getEncryptionMeta(status.getFileId());
        if (meta == null || !meta.hasCryptoKey()) {
          // Retry getting the crypto key if the cached meta does not have a valid crypto key.
          meta = getEncryptionMetaFromFooter(status);
          if (meta == null || !meta.hasCryptoKey()) {
            throw new IOException(
                "Can not open an encrypted file because the client can not get the crypto key.");
          }
          // Update the meta in cache with the new crypto key set.
          mFileSystemContext.putEncryptionMeta(status.getFileId(), meta);
        }
        inStreamOptions.setEncryptionMeta(meta);
      }
      if (inStreamOptions.isEncrypted()) {
        return CryptoFileInStream.create(status, inStreamOptions, mFileSystemContext);
      }
    }
    // ALLUXIO CS END
    return new FileInStream(status, inStreamOptions, mFileSystemContext);
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rename(src, dst, RenamePOptions.getDefaultInstance());
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(src);
    checkUri(dst);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Update this code on the master side.
      masterClient.rename(src, dst, options);
      LOG.debug("Renamed {} to {}, options: {}", src.getPath(), dst.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries)
      throws FileDoesNotExistException, IOException, AlluxioException {
    setAcl(path, action, entries, SetAclPOptions.getDefaultInstance());
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.setAcl(path, action, entries, options);
      LOG.debug("Set ACL for {}, entries: {} options: {}", path.getPath(), entries, options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void setAttribute(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    setAttribute(path, SetAttributePOptions.getDefaultInstance());
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.setAttribute(path, options);
      LOG.debug("Set attributes for {}, options: {}", path.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Starts the active syncing process on an Alluxio path.
   *
   * @param path the path to sync
   */
  @Override
  public void startSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.startSync(path);
      LOG.debug("Start syncing for {}", path.getPath());
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Stops the active syncing process on an Alluxio path.
   * @param path the path to stop syncing
   */
  @Override
  public void stopSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.stopSync(path);
      LOG.debug("Stop syncing for {}", path.getPath());
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void unmount(AlluxioURI path) throws IOException, AlluxioException {
    unmount(path, UnmountPOptions.getDefaultInstance());
  }

  @Override
  public void unmount(AlluxioURI path, UnmountPOptions options)
      throws IOException, AlluxioException {
    checkUri(path);
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.unmount(path);
      LOG.debug("Unmounted {}, options: {}", path.getPath(), options);
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Checks an {@link AlluxioURI} for scheme and authority information. Warn the user and throw an
   * exception if necessary.
   */
  private static void checkUri(AlluxioURI uri) {
    if (uri.hasScheme()) {
      String warnMsg = "The URI scheme \"{}\" is ignored and not required in URIs passed to"
          + " the Alluxio Filesystem client.";
      switch (uri.getScheme()) {
        case Constants.SCHEME:
          LOG.warn(warnMsg, Constants.SCHEME);
          break;
        case Constants.SCHEME_FT:
          LOG.warn(warnMsg, Constants.SCHEME_FT);
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Scheme %s:// in AlluxioURI is invalid. Schemes in filesystem"
                  + " operations are ignored. \"alluxio://\" or no scheme at all is valid.",
                  uri.getScheme()));
      }
    }

    if (uri.hasAuthority()) {
      LOG.warn("The URI authority (hostname and port) is ignored and not required in URIs passed "
          + "to the Alluxio Filesystem client.");
      /* Even if we choose to log the warning, check if the Configuration host matches what the
       * user passes. If not, throw an exception letting the user know they don't match.
       */
      Authority configured = MasterInquireClient.Factory.create().getConnectDetails().toAuthority();
      if (!configured.equals(uri.getAuthority())) {
        throw new IllegalArgumentException(
            String.format("The URI authority %s does not match the configured " + "value of %s.",
                uri.getAuthority(), configured));
      }
    }
    return;
  }
}
