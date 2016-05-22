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
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.job.benchmark.compatibility.Operation;

import org.apache.commons.lang3.Validate;

/**
 * Operation involving creating inodes.
 */
public final class CreateOperation implements Operation {
  private final FileSystem mFs;
  private final AlluxioURI mCreateUri = new AlluxioURI("/create");

  public CreateOperation(FileSystem fs) {
    mFs = fs;
  }

  @Override
  public void generate() throws Exception {
    // InodeDirectoryEntry - DEFAULT
    // InodeFileEntry - DEFAULT
    // InodeDirectoryIdGeneratorEntry
    // InodeLastModificationTimeEntry
    FileOutStream out = mFs.createFile(mCreateUri.join("f_default"));
    // BlockContainerIdGeneratorEntry
    // BlockInfoEntry - DEFAULT
    for (int i = 0; i < 100; i++) {
      out.write(i);
    }
    // CompleteFileEntry
    out.close();


    // InodeFileEntry.block_size_bytes
    out = mFs.createFile(mCreateUri.join("f_blocksize"),
        CreateFileOptions.defaults().setBlockSizeBytes(10));
    // BlockInfoEntry.length
    for (int i = 0; i < 1000; i++) {
      out.write(i);
    }
    out.close();


    // InodeFileEntry.ttl
    out = mFs.createFile(mCreateUri.join("f_ttl"),
        CreateFileOptions.defaults().setTtl(123456789L));
    for (int i = 0; i < 100; i++) {
      out.write(i);
    }
    out.close();

    // InodeDirectoryEntry.persistence_state
    // InodeFileEntry.persistence_state
    // PersistDirectoryEntry
    out = mFs.createFile(mCreateUri.join("d_persist1").join("d_persist2").join("f_persist"),
        CreateFileOptions.defaults().setWriteType(
            WriteType.CACHE_THROUGH).setRecursive(true));
    for (int i = 0; i < 100; i++) {
      out.write(i);
    }
    out.close();
  }

  @Override
  public void validate() throws Exception {
    URIStatus status;

    Validate.isTrue(mFs.exists(mCreateUri.join("f_default")));
    status = mFs.getStatus(mCreateUri.join("f_default"));
    Validate.isTrue(status.getLength() == 100);

    Validate.isTrue(mFs.exists(mCreateUri.join("f_blocksize")));
    status = mFs.getStatus(mCreateUri.join("f_blocksize"));
    Validate.isTrue(status.getLength() == 1000);
    Validate.isTrue(status.getBlockSizeBytes() == 10);
    Validate.isTrue(status.getBlockIds().size() == status.getLength() / status.getBlockSizeBytes());

    Validate.isTrue(mFs.exists(mCreateUri.join("f_ttl")));
    status = mFs.getStatus(mCreateUri.join("f_ttl"));
    Validate.isTrue(status.getTtl() == 123456789L);

    status = mFs.getStatus(mCreateUri.join("d_persist1"));
    Validate.isTrue(status.getPersistenceState().equals("PERSISTED"));

    status = mFs.getStatus(mCreateUri.join("d_persist1").join("d_persist2"));
    Validate.isTrue(status.getPersistenceState().equals("PERSISTED"));

    status = mFs.getStatus(mCreateUri.join("d_persist1").join("d_persist2").join("f_persist"));
    Validate.isTrue(status.getPersistenceState().equals("PERSISTED"));
  }

}
