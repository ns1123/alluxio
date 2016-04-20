/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Version;
import alluxio.security.authentication.AuthenticatedThriftServer;
import alluxio.security.authentication.TransportProvider;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.job.JobManagerWorker;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Entry point for the Alluxio job manager worker program. This class is responsible for
 * initializing the different workers that are configured to run.
 */
@NotThreadSafe
public final class AlluxioJobManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static AlluxioJobManagerWorker sAlluxioJobManagerWorker = null;

  /**
   * Main method for Alluxio Job Manager Worker.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    checkArgs(args);
    AlluxioJobManagerWorker worker = get();
    try {
      worker.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception while running worker, stopping it and exiting.", e);
      try {
        worker.stop();
      } catch (Exception ex) {
        // continue to exit
        LOG.error("Uncaught exception while stopping worker, simply exiting.", ex);
      }
      System.exit(-1);
    }
  }

  /**
   * Returns a handle to the Alluxio job manager worker instance.
   *
   * @return Alluxio job manager handle
   */
  public static synchronized AlluxioJobManagerWorker get() {
    if (sAlluxioJobManagerWorker == null) {
      sAlluxioJobManagerWorker = new AlluxioJobManagerWorker();
    }
    return sAlluxioJobManagerWorker;
  }

  private Configuration mConfiguration;

  /** The job manager worker. */
  private JobManagerWorker mJobManagerWorker;

  /** A list of extra workers to launch based on service loader. */
  private List<Worker> mAdditionalWorkers;

  /** Whether the worker is serving the RPC server. */
  private boolean mIsServingRPC = false;

  /** The transport provider to create thrift client transport. */
  private TransportProvider mTransportProvider;

  /** Thread pool for thrift. */
  private AuthenticatedThriftServer mThriftServer;

  /** Server socket for thrift. */
  private TServerSocket mThriftServerSocket;

  /** RPC local port for thrift. */
  private int mRPCPort;

  /** The address for the rpc server. */
  private InetSocketAddress mWorkerAddress;

  /** Net address of this worker. */
  private WorkerNetAddress mNetAddress;

  /** Worker start time in milliseconds. */
  private long mStartTimeMs;

  /**
   * Constructor of {@link AlluxioJobManagerWorker}.
   */
  public AlluxioJobManagerWorker() {
    try {
      mStartTimeMs = System.currentTimeMillis();
      mConfiguration = WorkerContext.getConf();

      mJobManagerWorker = new JobManagerWorker();

      mAdditionalWorkers = Lists.newArrayList();
      List<? extends Worker> workers = Lists.newArrayList(mJobManagerWorker);

      // Discover and register the available factories
      // NOTE: ClassLoader is explicitly specified so we don't need to set ContextClassLoader
      ServiceLoader<WorkerFactory> discoveredMasterFactories =
          ServiceLoader.load(WorkerFactory.class, WorkerFactory.class.getClassLoader());
      for (WorkerFactory factory : discoveredMasterFactories) {
        Worker worker = factory.create(workers);
        if (worker != null) {
          mAdditionalWorkers.add(worker);
        }
      }

      // Setup Thrift server
      mTransportProvider = TransportProvider.Factory.create(mConfiguration);
      mThriftServerSocket = createThriftServerSocket();
      mRPCPort = NetworkAddressUtils.getThriftPort(mThriftServerSocket);
      // Reset worker RPC port based on assigned port number
      mConfiguration.set(Constants.WORKER_RPC_PORT, Integer.toString(mRPCPort));
      mThriftServer = createThriftServer();

      mWorkerAddress =
          NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_RPC,
              mConfiguration);
    } catch (Exception e) {
      LOG.error("Failed to initialize {}", this.getClass().getName(), e);
      System.exit(-1);
    }
  }

  /**
   * @return the start time of the worker in milliseconds
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the uptime of the worker in milliseconds
   */
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  /**
   * @return this worker's rpc address
   */
  public InetSocketAddress getWorkerAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the worker RPC service bind host
   */
  public String getRPCBindHost() {
    return NetworkAddressUtils.getThriftSocket(mThriftServerSocket).getInetAddress()
        .getHostAddress();
  }

  /**
   * @return the worker RPC service port
   */
  public int getRPCLocalPort() {
    return mRPCPort;
  }

  /**
   * @return the block worker
   */
  public JobManagerWorker getJobManagerWorker() {
    return mJobManagerWorker;
  }

  /**
   * Gets this worker's {@link WorkerNetAddress}, which is the worker's hostname, rpc
   * server port, data server port, and web server port.
   *
   * @return the worker's net address
   */
  public WorkerNetAddress getNetAddress() {
    return mNetAddress;
  }

  /**
   * Starts the Alluxio worker server.
   *
   * @throws Exception if the workers fail to start
   */
  public void start() throws Exception {
    // NOTE: the order to start different services is sensitive. If you change it, do it cautiously.

    // Set updated net address for this worker in context
    // Requirement: RPC, web, and dataserver ports are updated
    // Consequence: create a NetAddress object and set it into WorkerContext
    mNetAddress =
        new WorkerNetAddress()
            .setHost(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, mConfiguration))
            .setRpcPort(mConfiguration.getInt(Constants.WORKER_RPC_PORT))
            .setDataPort(mConfiguration.getInt(Constants.WORKER_DATA_PORT))
            .setWebPort(mConfiguration.getInt(Constants.WORKER_WEB_PORT));
    WorkerContext.setWorkerNetAddress(mNetAddress);

    // Start each worker
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();
    LOG.info("Started worker with id {}", WorkerIdRegistry.getWorkerId());

    mIsServingRPC = true;

    // Start serving RPC, this will block
    LOG.info("Alluxio Worker version {} started @ {}", Version.VERSION, mWorkerAddress);
    mThriftServer.serve();
    LOG.info("Alluxio Worker version {} ended @ {}", Version.VERSION, mWorkerAddress);
  }

  /**
   * Stops the Alluxio job manager worker server.
   *
   * @throws Exception if the workers fail to stop
   */
  public void stop() throws Exception {
    if (mIsServingRPC) {
      LOG.info("Stopping RPC server on Alluxio Worker @ {}", mWorkerAddress);
      stopServing();
      stopWorkers();
      mIsServingRPC = false;
    } else {
      LOG.info("Stopping Alluxio Worker @ {}", mWorkerAddress);
    }
  }

  private void startWorkers() throws Exception {
    mJobManagerWorker.start();
    // start additional workers
    for (Worker worker : mAdditionalWorkers) {
      worker.start();
    }
  }

  private void stopWorkers() throws Exception {
    // stop additional workers
    for (Worker worker : mAdditionalWorkers) {
      worker.stop();
    }
    mJobManagerWorker.start();
  }

  private void stopServing() {
    mThriftServer.stop();
    mThriftServerSocket.close();
  }

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
    }
  }

  /**
   * Helper method to create a {@link AuthenticatedThriftServer} for handling incoming RPC requests.
   *
   * @return a thrift server
   */
  private AuthenticatedThriftServer createThriftServer() {
    int minWorkerThreads = mConfiguration.getInt(Constants.WORKER_WORKER_BLOCK_THREADS_MIN);
    int maxWorkerThreads = mConfiguration.getInt(Constants.WORKER_WORKER_BLOCK_THREADS_MAX);
    TMultiplexedProcessor processor = new TMultiplexedProcessor();

    registerServices(processor, mJobManagerWorker.getServices());
    // register additional workers for RPC service
    for (Worker worker: mAdditionalWorkers) {
      registerServices(processor, worker.getServices());
    }

    // Return a TTransportFactory based on the authentication type
    TTransportFactory tTransportFactory;
    try {
      tTransportFactory = mTransportProvider.getServerTransportFactory();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(mThriftServerSocket)
        .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads).processor(processor)
        .transportFactory(tTransportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true));
    if (WorkerContext.getConf().getBoolean(Constants.IN_TEST_MODE)) {
      args.stopTimeoutVal = 0;
    } else {
      args.stopTimeoutVal = Constants.THRIFT_STOP_TIMEOUT_SECONDS;
    }
    return new AuthenticatedThriftServer(mConfiguration, args);
  }

  /**
   * Helper method to create a {@link TServerSocket} for the RPC server.
   *
   * @return a thrift server socket
   */
  private TServerSocket createThriftServerSocket() {
    try {
      return new TServerSocket(
          NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, mConfiguration));
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Verifies that no parameters are passed in.
   *
   * @param args command line arguments
   */
  private static void checkArgs(String[] args) {
    if (args.length != 0) {
      LOG.info("Usage: java AlluxioWorker");
      System.exit(-1);
    }
  }
}
