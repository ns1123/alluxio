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
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The interface layer to communicate with Alluxio. Now Alluxio Client APIs may change and this
 * layer can keep the modifications  in this single file.
 */
public final class AlluxioFS implements AbstractFS {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFS.class);

  /**
   * @return a new AlluxioFS object
   */
  public static alluxio.job.fs.AlluxioFS get() {
    return new alluxio.job.fs.AlluxioFS();
  }

  private FileSystem mFs;

  private static byte[] sBuffer = new byte[Constants.MB];

  private AlluxioFS() {
    mFs = FileSystem.Factory.get();
  }

  @Override
  public void close() throws IOException {}

  @Override
  public OutputStream create(String path, long blockSizeByte) throws IOException {
    return create(path, blockSizeByte, WriteType.valueOf("MUST_CACHE"));
  }

  @Override
  public OutputStream create(String path, long blockSizeByte, WriteType writeType)
      throws IOException {
    return create(path, blockSizeByte, writeType, false);
  }

  @Override
  public OutputStream create(String path, long blockSizeByte, WriteType writeType,
      boolean recursive) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      return mFs.createFile(uri,
          CreateFileOptions.defaults().setBlockSizeBytes(blockSizeByte).setWriteType(writeType)
              .setRecursive(recursive));
    } catch (FileAlreadyExistsException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public OutputStream create(String path, short replication) throws IOException {
    long size = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    return create(path, (int) size);
  }

  @Override
  public void createDirectory(String path, WriteType writeType) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      mFs.createDirectory(uri,
          CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true)
              .setWriteType(writeType));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void createEmptyFile(String path, WriteType writeType) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      mFs.createFile(uri, CreateFileOptions.defaults().setWriteType(writeType).setRecursive(true))
          .close();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void listStatusAndIgnore(String path) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      mFs.listStatus(uri, ListStatusOptions.defaults());
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void randomReads(String path, long fileSize, int bytesToRead, int n) throws IOException {
    FileInStream inputStream = null;
    try {
      inputStream = mFs.openFile(new AlluxioURI(path));
      Random random = new Random();
      for (int i = 0; i < n; i++) {
        // Note that when fileSize is large enough (say ~PBs), the seek pos might not be perfectly
        // uniformly distributed.
        long pos = 0;
        do {
          pos = random.nextLong() % fileSize;
        } while (pos < 0);
        try (Timer.Context context = Metrics.RANDOM_READ_TOTAL.time()) {
          int bytesLeft = bytesToRead;
          try (Timer.Context contextRead = Metrics.RANDOM_READ_READ.time()) {
            while (bytesLeft > 0) {
              int bytesRead;
              bytesRead =
                  inputStream.positionedRead(pos, sBuffer, 0, Math.min(bytesLeft, sBuffer.length));
              if (bytesRead == -1) {
                break;
              }
              bytesLeft -= bytesRead;
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
      Metrics.RANDOM_READ_ERROR.inc();
      throw new IOException(e);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
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

  /**
   * Frees the path from Alluxio space.
   *
   * @param path the path to the file
   * @param recursive whether the directory content should be recursively freed
   * @throws IOException if an unexpected Alluxio exception is thrown
   */
  public void free(String path, boolean recursive) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    try {
      mFs.free(uri, FreeOptions.defaults().setRecursive(recursive));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * Class that contains metrics about AlluxioFS.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Timer RANDOM_READ_TOTAL = MetricsSystem.METRIC_REGISTRY
        .timer(MetricsSystem.getMetricNameWithUniqueId("microbench", "RandomReadTotalAlluxio"));
    private static final Timer RANDOM_READ_SEEK = MetricsSystem.METRIC_REGISTRY
        .timer(MetricsSystem.getMetricNameWithUniqueId("microbench", "RandomReadSeekAlluxio"));
    private static final Timer RANDOM_READ_READ = MetricsSystem.METRIC_REGISTRY
        .timer(MetricsSystem.getMetricNameWithUniqueId("microbench", "RandomReadReadAlluxio"));
    private static final Counter RANDOM_READ_ERROR = MetricsSystem.METRIC_REGISTRY
        .counter(MetricsSystem.getMetricNameWithUniqueId("microbench", "RandomReadErrorAlluxio"));

    private Metrics() {} // prevent instantiation
  }
}
