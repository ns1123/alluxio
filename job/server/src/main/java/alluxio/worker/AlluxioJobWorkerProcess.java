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

package alluxio.worker;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.underfs.JobUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.JobWorkerWebServer;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for initializing the different workers that are configured to run.
 */
@NotThreadSafe
public final class AlluxioJobWorkerProcess implements JobWorkerProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioJobWorkerProcess.class);

  /** The job worker. */
  private JobWorker mJobWorker;

  /** RPC local port for gRPC. */
  private int mRPCPort;

<<<<<<< HEAD
  /** Thread pool for thrift. */
  // ALLUXIO CS REPLACE
  // private TThreadPoolServer mThriftServer;
  // ALLUXIO CS WITH
  private alluxio.security.authentication.AuthenticatedThriftServer mThriftServer;
  // ALLUXIO CS END
=======
  /** gRPC server. */
  private GrpcServer mGrpcServer;
>>>>>>> 8cc5a292f4c6e38ed0066ce5bd700cc946dc3803

  /** Used for auto binding. **/
  private ServerSocket mBindSocket;

  /** The address for the rpc server. */
  private InetSocketAddress mRpcAddress;

  /** Worker start time in milliseconds. */
  private long mStartTimeMs;

  /** The web ui server. */
  private JobWorkerWebServer mWebServer = null;

  /** The manager for all ufs. */
  private UfsManager mUfsManager;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);

  /**
   * Constructor of {@link AlluxioJobWorker}.
   */
  AlluxioJobWorkerProcess() {
    try {
      mStartTimeMs = System.currentTimeMillis();
      mUfsManager = new JobUfsManager();
      mJobWorker = new JobWorker(mUfsManager);

      // Setup web server
      mWebServer = new JobWorkerWebServer(ServiceType.JOB_WORKER_WEB.getServiceName(),
          NetworkAddressUtils.getBindAddress(ServiceType.JOB_WORKER_WEB), this);

      // Random port binding.
      InetSocketAddress configuredBindAddress =
              NetworkAddressUtils.getBindAddress(ServiceType.JOB_WORKER_RPC);
      if (configuredBindAddress.getPort() == 0) {
        mBindSocket = new ServerSocket(0);
        mRPCPort = mBindSocket.getLocalPort();
      } else {
        mRPCPort = configuredBindAddress.getPort();
      }
      // Reset worker RPC port based on assigned port number
      Configuration.set(PropertyKey.JOB_WORKER_RPC_PORT, Integer.toString(mRPCPort));
      mRpcAddress =
          NetworkAddressUtils.getConnectAddress(ServiceType.JOB_WORKER_RPC);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcAddress;
  }

  @Override
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  @Override
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  @Override
  public InetSocketAddress getWebAddress() {
    if (mWebServer != null) {
      return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
    }
    return null;
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          () -> isServing()
              && mWebServer != null
              && mWebServer.getServer().isRunning(),
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  @Override
  public void start() throws Exception {
    // NOTE: the order to start different services is sensitive. If you change it, do it cautiously.

    // Start serving the web server, this will not block.
    mWebServer.addHandler(mMetricsServlet.getHandler());
    mWebServer.start();

    // Start each worker
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();
    LOG.info("Started {} with id {}", this, JobWorkerIdRegistry.getWorkerId());

    // Start serving RPC, this will block
    LOG.info("Alluxio job worker version {} started. "
            + "bindHost={}, connectHost={}, rpcPort={}, webPort={}",
        RuntimeConstants.VERSION,
        NetworkAddressUtils.getBindAddress(ServiceType.JOB_WORKER_RPC),
        NetworkAddressUtils.getConnectAddress(ServiceType.JOB_WORKER_RPC),
        NetworkAddressUtils.getPort(ServiceType.JOB_WORKER_RPC),
        NetworkAddressUtils.getPort(ServiceType.JOB_WORKER_WEB));

    startServingRPCServer();
    LOG.info("Alluxio job worker ended");
  }

  private void startServingRPCServer() {
    try {
      if (mBindSocket != null) {
        // Socket opened for auto bind.
        // Close it.
        mBindSocket.close();
      }

      LOG.info("Starting gRPC server on address {}", mRpcAddress);
      GrpcServerBuilder serverBuilder = GrpcServerBuilder.forAddress(mRpcAddress);

      for (Map.Entry<alluxio.grpc.ServiceType, GrpcService> serviceEntry : mJobWorker.getServices()
          .entrySet()) {
        LOG.info("Registered service:{}", serviceEntry.getKey().name());
        serverBuilder.addService(serviceEntry.getValue());
      }

      mGrpcServer = serverBuilder.build().start();
      LOG.info("Started gRPC server on address {}", mRpcAddress);

      // Wait until the server is shut down.
      mGrpcServer.awaitTermination();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isServing() {
    return mGrpcServer != null && mGrpcServer.isServing();
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping RPC server on {} @ {}", this, mRpcAddress);
    if (isServing()) {
      stopServing();
      stopWorkers();
    }
  }

  @Override
  public WorkerNetAddress getAddress() {
    return new WorkerNetAddress()
        .setHost(NetworkAddressUtils.getConnectHost(ServiceType.JOB_WORKER_RPC))
        .setRpcPort(Configuration.getInt(PropertyKey.JOB_WORKER_RPC_PORT))
        .setDataPort(Configuration.getInt(PropertyKey.JOB_WORKER_DATA_PORT))
        // ALLUXIO CS ADD
        .setSecureRpcPort(Configuration.getInt(PropertyKey.JOB_WORKER_SECURE_RPC_PORT))
        // ALLUXIO CS END
        .setWebPort(Configuration.getInt(PropertyKey.JOB_WORKER_WEB_PORT));
  }

  private void startWorkers() throws Exception {
    mJobWorker.start(getAddress());
  }

  private void stopWorkers() throws Exception {
    // stop additional workers
    mJobWorker.stop();
  }

  private void stopServing() {
    if (isServing()) {
      if (!mGrpcServer.shutdown()) {
        LOG.warn("RPC server shutdown timed out.");
      }
    }
    try {
      mWebServer.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop web server", e);
    }
  }

<<<<<<< HEAD
  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
    }
  }

  /**
   *
   * Helper method to create a thrift server for handling incoming RPC requests.
   *
   * @return a thrift server
   */
  // ALLUXIO CS REPLACE
  // private TThreadPoolServer createThriftServer() {
  // ALLUXIO CS WITH
  private alluxio.security.authentication.AuthenticatedThriftServer createThriftServer() {
  // ALLUXIO CS END
    int minWorkerThreads = Configuration.getInt(PropertyKey.WORKER_BLOCK_THREADS_MIN);
    int maxWorkerThreads = Configuration.getInt(PropertyKey.WORKER_BLOCK_THREADS_MAX);
    TMultiplexedProcessor processor = new TMultiplexedProcessor();

    registerServices(processor, mJobWorker.getServices());

    // Return a TTransportFactory based on the authentication type
    TTransportFactory tTransportFactory;
    try {
      String serverName = NetworkAddressUtils.getConnectHost(ServiceType.JOB_WORKER_RPC);
      tTransportFactory = mTransportProvider.getServerTransportFactory(serverName);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(mThriftServerSocket)
        .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads).processor(processor)
        .transportFactory(tTransportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true));
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      args.stopTimeoutVal = 0;
    } else {
      args.stopTimeoutVal = Constants.THRIFT_STOP_TIMEOUT_SECONDS;
    }
    // ALLUXIO CS REPLACE
    // return new TThreadPoolServer(args);
    // ALLUXIO CS WITH
    args.executorService(alluxio.concurrent.Executors
        .createDefaultExecutorServiceWithSecurityOn(args));

    return new alluxio.security.authentication.AuthenticatedThriftServer(args);
    // ALLUXIO CS END
  }

  /**
   * Helper method to create a {@link TServerSocket} for the RPC server.
   *
   * @return a thrift server socket
   */
  private TServerSocket createThriftServerSocket() {
    try {
      return new TServerSocket(NetworkAddressUtils.getBindAddress(ServiceType.JOB_WORKER_RPC));
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

=======
>>>>>>> 8cc5a292f4c6e38ed0066ce5bd700cc946dc3803
  @Override
  public String toString() {
    return "Alluxio job worker";
  }
}
