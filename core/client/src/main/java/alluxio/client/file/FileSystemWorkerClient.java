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
<<<<<<< HEAD
||||||| merged common ancestors
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
=======
import alluxio.Configuration;
import alluxio.PropertyKey;
>>>>>>> enterprise-1.4
import alluxio.client.file.options.CancelUfsFileOptions;
import alluxio.client.file.options.CloseUfsFileOptions;
import alluxio.client.file.options.CompleteUfsFileOptions;
import alluxio.client.file.options.CreateUfsFileOptions;
import alluxio.client.file.options.OpenUfsFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.retry.RetryPolicy;
import alluxio.wire.WorkerNetAddress;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Interface for an Alluxio file system worker client.
 */
<<<<<<< HEAD
public interface FileSystemWorkerClient extends Closeable {
||||||| merged common ancestors
// TODO(calvin): Session logic can be abstracted
@ThreadSafe
public class FileSystemWorkerClient
    extends AbstractThriftClient<FileSystemWorkerClientService.Client> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final ScheduledExecutorService HEARTBEAT_POOL = Executors.newScheduledThreadPool(
      Configuration.getInt(PropertyKey.USER_FILE_WORKER_CLIENT_THREADS),
      ThreadFactoryUtils.build("file-worker-heartbeat-%d", true));
  private static final ExecutorService HEARTBEAT_CANCEL_POOL = Executors.newFixedThreadPool(5,
      ThreadFactoryUtils.build("file-worker-heartbeat-cancel-%d", true));

  // Tracks the number of active heartbeats.
  private static final AtomicInteger NUM_ACTIVE_SESSIONS = new AtomicInteger(0);

  private final FileSystemWorkerThriftClientPool mClientPool;
  private final FileSystemWorkerThriftClientPool mClientHeartbeatPool;

  /** The current session id, managed by the caller. */
  private final long mSessionId;

  /** Address of the data server on the worker. */
  private final InetSocketAddress mWorkerDataServerAddress;

  private final ScheduledFuture<?> mHeartbeat;
=======
// TODO(calvin): Session logic can be abstracted
@ThreadSafe
public class FileSystemWorkerClient
    extends AbstractThriftClient<FileSystemWorkerClientService.Client> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWorkerClient.class);

  private static final ScheduledExecutorService HEARTBEAT_POOL = Executors.newScheduledThreadPool(
      Configuration.getInt(PropertyKey.USER_FILE_WORKER_CLIENT_THREADS),
      ThreadFactoryUtils.build("file-worker-heartbeat-%d", true));
  private static final ExecutorService HEARTBEAT_CANCEL_POOL = Executors.newFixedThreadPool(5,
      ThreadFactoryUtils.build("file-worker-heartbeat-cancel-%d", true));

  // Tracks the number of active heartbeats.
  private static final AtomicInteger NUM_ACTIVE_SESSIONS = new AtomicInteger(0);

  private final FileSystemWorkerThriftClientPool mClientPool;
  private final FileSystemWorkerThriftClientPool mClientHeartbeatPool;

  /** The current session id, managed by the caller. */
  private final long mSessionId;

  /** Address of the data server on the worker. */
  private final InetSocketAddress mWorkerDataServerAddress;

  private final ScheduledFuture<?> mHeartbeat;
>>>>>>> enterprise-1.4

  /**
   * Factory for {@link FileSystemWorkerClient}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory method for {@link FileSystemWorkerClient}.
     *
     * @param clientPool the client pool
     * @param clientHeartbeatPool the client pool for heartbeat
     * @param workerNetAddress the worker address to connect to
     * @param sessionId the session id to use, this should be unique
     * @return new {@link FileSystemWorkerClient} instance
     * @throws IOException if it fails to register the session with the worker specified
     */
    public static FileSystemWorkerClient create(FileSystemWorkerThriftClientPool clientPool,
        FileSystemWorkerThriftClientPool clientHeartbeatPool, WorkerNetAddress workerNetAddress,
        long sessionId) throws IOException {
      return RetryHandlingFileSystemWorkerClient
          .create(clientPool, clientHeartbeatPool, workerNetAddress, sessionId);
    }
  }

  /**
   * Cancels the file currently being written with the specified id. This file must have also
   * been created through this client. The file id will be invalid after this call.
   *
   * @param tempUfsFileId the worker specific id of the file to cancel
   * @param options method options
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  void cancelUfsFile(final long tempUfsFileId, final CancelUfsFileOptions options)
      throws AlluxioException, IOException;

  /**
   * Closes the file currently being written with the specified id. This file must have also
   * been opened through this client. The file id will be invalid after this call.
   *
   * @param tempUfsFileId the worker specific id of the file to close
   * @param options method options
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  void closeUfsFile(final long tempUfsFileId, final CloseUfsFileOptions options)
      throws AlluxioException, IOException;

  /**
   * Completes the file currently being written with the specified id. This file must have also
   * been created through this client. The file id will be invalid after this call.
   *
   * @param tempUfsFileId the worker specific id of the file to complete
   * @param options method options
   * @return the file size of the completed file
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  long completeUfsFile(final long tempUfsFileId, final CompleteUfsFileOptions options)
      throws AlluxioException, IOException;

  /**
   * Creates a new file in the UFS with the given path.
   *
   * @param path the path in the UFS to create, must not already exist
   * @param options method options
   * @return the worker specific file id to reference the created file
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  long createUfsFile(final AlluxioURI path, final CreateUfsFileOptions options)
      throws AlluxioException, IOException;

  /**
   * @return the data server address of the worker this client is connected to
   */
  InetSocketAddress getWorkerDataServerAddress();

  /**
   * Opens an existing file in the UFS with the given path.
   *
   * @param path the path in the UFS to open, must exist
   * @param options method options
   * @return the worker specific file id to reference the opened file
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  long openUfsFile(final AlluxioURI path, final OpenUfsFileOptions options)
      throws AlluxioException, IOException;
  /**
   * Sends a session heartbeat to the worker. This renews the client's lease on resources such as
   * temporary files.
   *
   * @param retryPolicy the retry policy to use
   * @throws IOException if an I/O error occurs
   * @throws InterruptedException if this thread is interrupted
   */
  void sessionHeartbeat(RetryPolicy retryPolicy) throws IOException, InterruptedException;
}
