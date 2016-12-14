/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.compatibility.operations;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.compatibility.Operation;

import org.apache.commons.lang3.Validate;

/**
 * Operation involving creating inodes.
 */
public final class CreateOperation implements Operation {
  private static final int FILE_SIZE = 100;
  private static final int BIG_FILE_SIZE = 1000;
  private static final int SMALL_BLOCK_SIZE = 10;
  private static final long TTL = 123456789L;

  private final FileSystem mFs;
  private final AlluxioURI mCreateUri = new AlluxioURI("/create");
  private final AlluxioURI mFileDefault = mCreateUri.join("f_default");
  private final AlluxioURI mFileBlockSize = mCreateUri.join("f_blocksize");
  private final AlluxioURI mFileTtl = mCreateUri.join("f_ttl");
  private final AlluxioURI mFileNested1 = mCreateUri.join("d_persist1");
  private final AlluxioURI mFileNested2 = mFileNested1.join("d_persist2");
  private final AlluxioURI mFileNestedPersist = mFileNested2.join("f_persist");

  /**
   * Creates a new {@link CreateOperation}.
   *
   * @param context the {@link JobWorkerContext} to use
   */
  public CreateOperation(JobWorkerContext context) {
    mFs = BaseFileSystem.get(FileSystemContext.INSTANCE);
  }

  @Override
  public void generate() throws Exception {
    // InodeDirectoryEntry - DEFAULT
    // InodeFileEntry - DEFAULT
    // InodeDirectoryIdGeneratorEntry
    // InodeLastModificationTimeEntry
    FileOutStream out = mFs.createFile(mFileDefault);
    // BlockContainerIdGeneratorEntry
    // BlockInfoEntry - DEFAULT
    for (int i = 0; i < FILE_SIZE; i++) {
      out.write(i);
    }
    // CompleteFileEntry
    out.close();

    // InodeFileEntry.block_size_bytes
    out = mFs.createFile(mFileBlockSize,
        CreateFileOptions.defaults().setBlockSizeBytes(SMALL_BLOCK_SIZE));
    // BlockInfoEntry.length
    for (int i = 0; i < BIG_FILE_SIZE; i++) {
      out.write(i);
    }
    out.close();

    // InodeFileEntry.ttl
    out = mFs.createFile(mFileTtl, CreateFileOptions.defaults().setTtl(TTL));
    for (int i = 0; i < FILE_SIZE; i++) {
      out.write(i);
    }
    out.close();

    // InodeDirectoryEntry.persistence_state
    // InodeFileEntry.persistence_state
    // PersistDirectoryEntry
    out = mFs.createFile(mFileNestedPersist,
        CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH).setRecursive(true));
    for (int i = 0; i < FILE_SIZE; i++) {
      out.write(i);
    }
    out.close();
  }

  @Override
  public void validate() throws Exception {
    URIStatus status;

    Validate.isTrue(mFs.exists(mFileDefault));
    status = mFs.getStatus(mFileDefault);
    Validate.isTrue(status.getLength() == FILE_SIZE);

    Validate.isTrue(mFs.exists(mFileBlockSize));
    status = mFs.getStatus(mFileBlockSize);
    Validate.isTrue(status.getLength() == BIG_FILE_SIZE);
    Validate.isTrue(status.getBlockSizeBytes() == SMALL_BLOCK_SIZE);
    Validate.isTrue(status.getBlockIds().size() == status.getLength() / status.getBlockSizeBytes());

    Validate.isTrue(mFs.exists(mFileTtl));
    status = mFs.getStatus(mFileTtl);
    Validate.isTrue(status.getTtl() == TTL);

    status = mFs.getStatus(mFileNested1);
    Validate.isTrue(status.getPersistenceState().equals("PERSISTED"));

    status = mFs.getStatus(mFileNested2);
    Validate.isTrue(status.getPersistenceState().equals("PERSISTED"));

    status = mFs.getStatus(mFileNestedPersist);
    Validate.isTrue(status.getPersistenceState().equals("PERSISTED"));
  }
}
