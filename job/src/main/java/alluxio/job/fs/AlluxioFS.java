/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.fs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * The interface layer to communicate with Alluxio. Now Alluxio Client APIs may change and this
 * layer can keep the modifications  in this single file.
 */
public final class AlluxioFS implements AbstractFS {
  /**
   * @return a new AlluxioFS object
   */
  public static alluxio.job.fs.AlluxioFS get() {
    return new alluxio.job.fs.AlluxioFS();
  }

  private FileSystem mFs;

  private AlluxioFS() {
    mFs = FileSystem.Factory.get();
  }

  @Override
  public void close() throws IOException {}

  @Override
  public OutputStream create(String path) throws IOException {
    long size = new Configuration().getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    return create(path, (int) size);
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return create(path, blockSizeByte, "MUST_CACHE");
  }

  @Override
  public OutputStream create(String path, int blockSizeByte, String writeType) throws IOException {
    WriteType type = WriteType.valueOf(writeType);
    AlluxioURI uri = new AlluxioURI(path);
    try {
      return mFs.createFile(uri,
          CreateFileOptions.defaults().setBlockSizeBytes(blockSizeByte).setWriteType(type));
    } catch (FileAlreadyExistsException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean createEmptyFile(String path) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      if (mFs.exists(uri)) {
        return false;
      }
      return (mFs.createFile(uri) != null);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
    try {
      mFs.delete(new AlluxioURI(path), options);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    try {
      return mFs.exists(new AlluxioURI(path));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getLength(String path) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      if (!mFs.exists(uri)) {
        return 0;
      }
      return mFs.getStatus(uri).getLength();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      if (!mFs.exists(uri)) {
        return false;
      }
      return mFs.getStatus(uri).isFolder();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return !isDirectory(path);
  }

  @Override
  public List<String> listFullPath(String path) throws IOException {
    List<URIStatus> files;
    try {
      files = mFs.listStatus(new AlluxioURI(path));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    if (files == null) {
      return null;
    }
    ArrayList<String> ret = new ArrayList<String>(files.size());
    for (URIStatus fileInfo : files) {
      ret.add(fileInfo.getPath());
    }
    return ret;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      mFs.createDirectory(uri, CreateDirectoryOptions.defaults().setRecursive(createParent));
      return true;
    } catch (AlluxioException e) {
      return false;
    }
  }

  @Override
  public InputStream open(String path) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      return mFs.openFile(uri);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream open(String path, ReadType readType) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      return mFs.openFile(uri, OpenFileOptions.defaults().setReadType(readType));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    AlluxioURI srcURI = new AlluxioURI(src);
    AlluxioURI dstURI = new AlluxioURI(dst);
    try {
      mFs.rename(srcURI, dstURI);
      return true;
    } catch (AlluxioException e) {
      return false;
    }
  }
}
