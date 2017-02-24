/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.fs;

import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The interface layer to communicate with HDFS or Alluxio through HDFS API.
 */
public final class HDFSFS implements AbstractFS {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSFS.class);
  private static org.apache.hadoop.conf.Configuration sHadoopConf = hadoopConfig();

  /**
   * @return a new HDFSFS object of HDFS implementation
   */
  public static alluxio.job.fs.HDFSFS getHdfs() {
    return new alluxio.job.fs.HDFSFS(false);
  }

  /**
   * @return a new HDFSFS object of Alluxio implementation
   */
  public static alluxio.job.fs.HDFSFS getAlluxio() {
    return new alluxio.job.fs.HDFSFS(true);
  }

  private FileSystem mTfs;

  private static byte[] sBuffer = new byte[Constants.MB];

  private static org.apache.hadoop.conf.Configuration hadoopConfig() {
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    hadoopConf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
    hadoopConf.set("fs.alluxio-ft.impl", "alluxio.hadoop.FaultTolerantFileSystem");
    return hadoopConf;
  }

  private HDFSFS(boolean alluxioFs) {
    try {
      String masterAddr = alluxioFs ? alluxio.Configuration.get(PropertyKey.MASTER_ADDRESS)
          : alluxio.Configuration.get(PropertyKey.UNDERFS_ADDRESS);
      URI u = new URI(masterAddr);
      mTfs = FileSystem.get(u, sHadoopConf);
    } catch (IOException e) {
      LOG.error("Failed to get HDFS client", e);
      Throwables.propagate(e);
    } catch (URISyntaxException u) {
      LOG.error("Failed to parse underfs uri", u);
      Throwables.propagate(u);
    }
  }

  @Override
  public void close() throws IOException {
    mTfs.close();
  }

  @Override
  public OutputStream create(String path, long blockSizeByte) throws IOException {
    return create(path, blockSizeByte, WriteType.MUST_CACHE);
  }

  @Override
  public OutputStream create(String path, long blockSizeByte, WriteType writeType)
      throws IOException {
    return create(path, blockSizeByte, writeType, false);
  }

  @Override
  public OutputStream create(String path, long blockSizeByte, WriteType writeType,
      boolean recursive) throws IOException {
    Path p = new Path(path);
    return mTfs.create(p);
  }

  @Override
  public OutputStream create(String path, short replication) throws IOException {
    Path p = new Path(path);
    return mTfs.create(p, replication);
  }

  @Override
  public void createDirectory(String path, WriteType writeType) throws IOException {
    Path p = new Path(path);
    mTfs.mkdirs(p);
  }

  @Override
  public void createEmptyFile(String path, WriteType writeType) throws IOException {
    Path p = new Path(path);
    mTfs.create(p).close();
  }

  @Override
  public void listStatusAndIgnore(String path) throws IOException {
    mTfs.listStatus(new Path(path));
  }

  @Override
  public void randomReads(String path, long fileSize, int bytesToRead, int n) throws IOException {
    FSDataInputStream inputStream = mTfs.open(new Path(path));
    Random random = new Random();
    try {
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
              int bytesRead = inputStream.read(pos, sBuffer, 0, Math.min(bytesLeft, sBuffer.length));
              if (bytesRead <= 0) {
                break;
              }
              pos += bytesRead;
              bytesLeft -= bytesRead;
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
      Metrics.RANDOM_READ_ERROR.inc();
      throw e;
    } finally {
      inputStream.close();
    }
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    return mTfs.delete(new Path(path), recursive);
  }

  @Override
  public boolean exists(String path) throws IOException {
    return mTfs.exists(new Path(path));
  }

  @Override
  public long getLength(String path) throws IOException {
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      return 0;
    }
    return mTfs.getFileStatus(p).getLen();
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      return false;
    }
    return mTfs.getFileStatus(p).isDir();
  }

  @Override
  public boolean isFile(String path) throws IOException {
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      return false;
    }
    return !mTfs.getFileStatus(p).isDir();
  }

  @Override
  public List<String> listFullPath(String path) throws IOException {
    List<FileStatus> files = Arrays.asList(mTfs.listStatus(new Path(path)));
    if (files == null) {
      return null;
    }
    ArrayList<String> ret = new ArrayList<String>(files.size());
    for (FileStatus fileInfo : files) {
      ret.add(fileInfo.getPath().toString());
    }
    return ret;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    Path p = new Path(path);
    if (mTfs.exists(p)) {
      return false;
    }
    return mTfs.mkdirs(p);
  }

  @Override
  public InputStream open(String path) throws IOException {
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      throw new FileNotFoundException("File not exists " + path);
    }
    return mTfs.open(p);
  }

  @Override
  public InputStream open(String path, ReadType readType) throws IOException {
    // Read type not applicable
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      throw new FileNotFoundException("File not exists " + path);
    }
    return mTfs.open(p);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    Path srcPath = new Path(src);
    Path dstPath = new Path(dst);
    return mTfs.rename(srcPath, dstPath);
  }

  /**
   * Class that contains metrics about {@link HDFSFS}.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Timer RANDOM_READ_TOTAL = MetricsSystem.METRIC_REGISTRY
        .timer(MetricsSystem.getMetricNameWithUniqueId("microbench", "RandomReadTotalHDFS"));
    private static final Timer RANDOM_READ_SEEK = MetricsSystem.METRIC_REGISTRY
        .timer(MetricsSystem.getMetricNameWithUniqueId("microbench", "RandomReadSeekHDFS"));
    private static final Timer RANDOM_READ_READ = MetricsSystem.METRIC_REGISTRY
        .timer(MetricsSystem.getMetricNameWithUniqueId("microbench", "RandomReadReadHDFS"));
    private static final Counter RANDOM_READ_ERROR = MetricsSystem.METRIC_REGISTRY
        .counter(MetricsSystem.getMetricNameWithUniqueId("microbench", "RandomReadErrorHDFS"));

    private Metrics() {} // prevent instantiation
  }
}
