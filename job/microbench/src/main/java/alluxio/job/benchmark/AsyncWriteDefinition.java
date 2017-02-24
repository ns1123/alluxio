/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.job.fs.AlluxioFS;
import alluxio.job.util.SerializableVoid;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Async write micro benchmark that writes a file asynchronously in a thread. Each thread writes the
 * file to async-write/[task-id]/[thread-id].
 */
public final class AsyncWriteDefinition
    extends AbstractNoArgBenchmarkJobDefinition<AsyncWriteConfig, AsyncIOThroughputResult> {

  public static final String WRITE_DIR = "/async-write/";
  /** A queue tracks the total read byte per thread. */
  private ConcurrentLinkedQueue<Long> mInMemWriteTimeQueue = null;

  /**
   * Constructs a new {@link AsyncWriteDefinition}.
   */
  public AsyncWriteDefinition() {}

  @Override
  public String join(AsyncWriteConfig config, Map<WorkerInfo, AsyncIOThroughputResult> taskResults)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    double totalSync = 0.0;
    double totalAsync = 0.0;
    double totalTime = 0;
    for (AsyncIOThroughputResult result : taskResults.values()) {
      totalSync += result.getThroughput();
      totalTime += result.getDuration();
      totalAsync += result.getAsyncThroughput();
    }

    double syncThroughput = totalSync / taskResults.size();
    double asyncThroughput = totalAsync / taskResults.size();
    double duration = totalTime / taskResults.size();

    sb.append(String.format("Sync Throughput:%s (MB/s)%n",
        ReportFormatUtils.getStringValue(syncThroughput)));
    sb.append(String.format("Async Throughput:%s (MB/s)%n",
        ReportFormatUtils.getStringValue(asyncThroughput)));
    sb.append(String.format("Duration:%f (ms)%n", duration));
    if (config.isVerbose()) {
      sb.append(String.format("********** Task Configurations **********%n"));
      sb.append(config.toString());
      sb.append(String.format("%n********** Statistics **********%n"));
      sb.append(String.format("%nWorker\t\tThroughput(MB/s)"));
      for (Entry<WorkerInfo, AsyncIOThroughputResult> entry : taskResults.entrySet()) {
        sb.append(entry.getKey().getId() + "@" + entry.getKey().getAddress().getHost());
        sb.append(
            "\t\tthroughtput" + ReportFormatUtils.getStringValue(entry.getValue().getThroughput()));
        sb.append("\t\tasync throughtput"
            + ReportFormatUtils.getStringValue(entry.getValue().getAsyncThroughput()));
      }
    }

    return new BenchmarkEntry(DatabaseConstants.ASYNC_WRITE,
        ImmutableList.of("Duration", "SyncThroughput", "AsyncThroughput", "Comment"),
        ImmutableList.of("int", "float", "float", "text"),
        ImmutableMap.<String, Object>of("SyncThroughput", syncThroughput, "AsyncThroughput",
            asyncThroughput, "Duration", duration, "Comment", sb.toString())).toJson();
  }

  @Override
  protected void before(AsyncWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(fs, jobWorkerContext);
    // delete the directory if it exists
    if (fs.exists(path)) {
      fs.delete(path, true /* recursive */);
    }
    // create the directory
    fs.mkdirs(path, true /* recursive */);

    if (mInMemWriteTimeQueue == null) {
      mInMemWriteTimeQueue = new ConcurrentLinkedQueue<>();
    }
  }

  @Override
  protected void run(AsyncWriteConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext, int batch, int threadIndex) throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    // use the thread id as the file name
    String path = getWritePrefix(fs, jobWorkerContext) + "/" + threadIndex;

    long blockSize = config.getBlockSize();
    long bufferSize = FormatUtils.parseSpaceSize(config.getBufferSize());
    long fileSize = FormatUtils.parseSpaceSize(config.getFileSize());
    WriteType writeType = WriteType.ASYNC_THROUGH;
    OutputStream os = fs.create(path, blockSize, writeType);

    long startTimeNano = System.nanoTime();
    // write the file
    byte[] content = new byte[(int) bufferSize];
    Arrays.fill(content, (byte) 'a');
    long remain = fileSize;
    while (remain >= bufferSize) {
      os.write(content);
      remain -= bufferSize;
    }
    if (remain > 0) {
      os.write(content, 0, (int) remain);
    }

    os.close();
    long endTimeNano = System.nanoTime();
    mInMemWriteTimeQueue.add(endTimeNano - startTimeNano);

    waitForPersist(jobWorkerContext, new AlluxioURI(path), config);
  }

  void waitForPersist(final JobWorkerContext jobWorkerContext, final AlluxioURI path,
      AsyncWriteConfig config) {
    BenchmarkUtils.waitFor(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        try {
          FileSystem fileSystem = BaseFileSystem.get(FileSystemContext.INSTANCE);
          return fileSystem.getStatus(path).isPersisted();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }, (long) config.getPersistTimeout() * Constants.SECOND_MS);
  }

  @Override
  protected void after(AsyncWriteConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Delete the directory used by this task.
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(fs, jobWorkerContext);
    fs.delete(path, true /* recursive */);
  }

  @Override
  protected AsyncIOThroughputResult process(AsyncWriteConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    Preconditions.checkArgument(benchmarkThreadTimeList.size() == 1,
        "AsyncWrite only does one batch");
    // calc the average time
    long totalTimeNS = 0;
    for (long time : mInMemWriteTimeQueue) {
      totalTimeNS += time;
    }
    long totalAsyncTimeNS = 0;
    for (long asyncTime : benchmarkThreadTimeList.get(0)) {
      totalAsyncTimeNS += asyncTime;
    }
    long bytes = FormatUtils.parseSpaceSize(config.getFileSize()) * config.getThreadNum();
    double throughput =
        (bytes / (double) Constants.MB) / (totalTimeNS / (double) Constants.SECOND_NANO);
    double asyncThroughput =
        (bytes / (double) Constants.MB) / (totalAsyncTimeNS / (double) Constants.SECOND_NANO);
    double averageTimeMS = totalAsyncTimeNS / (double) benchmarkThreadTimeList.size()
        / Constants.SECOND_NANO * Constants.SECOND_MS;
    return new AsyncIOThroughputResult(throughput, asyncThroughput, averageTimeMS);
  }

  /**
   * Gets the write tasks working directory prefix.
   *
   * @param fs the file system
   * @param ctx the job worker context
   * @return the tasks working directory prefix
   */
  public static String getWritePrefix(AbstractFS fs, JobWorkerContext ctx) {
    String path = WRITE_DIR + ctx.getTaskId();
    // If the FS is not Alluxio, apply the Alluxio UNDERFS_ADDRESS prefix to the file path.
    // Thereforce, the UFS files are also written to the Alluxio mapped directory.
    if (!(fs instanceof AlluxioFS)) {
      path = Configuration.get(PropertyKey.UNDERFS_ADDRESS) + path;
    }
    return new StringBuilder().append(path).toString();
  }

  @Override
  public Class<AsyncWriteConfig> getJobConfigClass() {
    return AsyncWriteConfig.class;
  }
}
