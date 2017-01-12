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
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.concurrent.Executors;
import alluxio.security.authentication.AuthenticatedThriftServer;
import alluxio.security.authentication.TransportProvider;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
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
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for initializing the different workers that are configured to run.
 */
@NotThreadSafe
public final class DefaultAlluxioJobWorker implements AlluxioJobWorkerService {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The job worker. */
  private JobWorker mJobWorker;

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
  private InetSocketAddress mRpcAddress;

  /** Worker start time in milliseconds. */
  private long mStartTimeMs;

  /**
   * Constructor of {@link AlluxioJobWorker}.
   */
  public DefaultAlluxioJobWorker() {
    try {
      mStartTimeMs = System.currentTimeMillis();
      mJobWorker = new JobWorker();

      // Setup Thrift server
      mTransportProvider = TransportProvider.Factory.create();
      mThriftServerSocket = createThriftServerSocket();
      mRPCPort = NetworkAddressUtils.getThriftPort(mThriftServerSocket);
      // Reset worker RPC port based on assigned port number
      Configuration.set(PropertyKey.JOB_WORKER_RPC_PORT, Integer.toString(mRPCPort));
      mThriftServer = createThriftServer();

      mRpcAddress =
          NetworkAddressUtils.getConnectAddress(ServiceType.JOB_WORKER_RPC);
    } catch (Exception e) {
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
  public void waitForReady() {
    CommonUtils.waitFor("Alluxio job worker to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mThriftServer.isServing();
      }
    });
  }

  /**
   * @return the block worker
   */
  public JobWorker getJobWorker() {
    return mJobWorker;
  }

  /**
   * Starts the Alluxio worker server.
   *
   * @throws Exception if the workers fail to start
   */
  public void start() throws Exception {
    // NOTE: the order to start different services is sensitive. If you change it, do it cautiously.

    // Start each worker
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();
    LOG.info("Started job worker with id {}", JobWorkerIdRegistry.getWorkerId());

    mIsServingRPC = true;

    // Start serving RPC, this will block
    LOG.info("Alluxio Job Worker version {} started @ {}", RuntimeConstants.VERSION, mRpcAddress);
    mThriftServer.serve();
    LOG.info("Alluxio Job Worker version {} ended @ {}", RuntimeConstants.VERSION, mRpcAddress);
  }

  /**
   * Stops the Alluxio job worker server.
   *
   * @throws Exception if the workers fail to stop
   */
  public void stop() throws Exception {
    LOG.info("Stopping RPC server on Alluxio Job Worker @ {}", mRpcAddress);
    if (mIsServingRPC) {
      stopServing();
      stopWorkers();
      mIsServingRPC = false;
    }
  }

  private void startWorkers() throws Exception {
    mJobWorker.start();
  }

  private void stopWorkers() throws Exception {
    // stop additional workers
    mJobWorker.stop();
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
    int minWorkerThreads = Configuration.getInt(PropertyKey.WORKER_BLOCK_THREADS_MIN);
    int maxWorkerThreads = Configuration.getInt(PropertyKey.WORKER_BLOCK_THREADS_MAX);
    TMultiplexedProcessor processor = new TMultiplexedProcessor();

    registerServices(processor, mJobWorker.getServices());

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
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      args.stopTimeoutVal = 0;
    } else {
      args.stopTimeoutVal = Constants.THRIFT_STOP_TIMEOUT_SECONDS;
    }
    args.executorService(Executors.createDefaultExecutorServiceWithSecurityOn(args));
    return new AuthenticatedThriftServer(args);
  }

  /**
   * Helper method to create a {@link TServerSocket} for the RPC server.
   *
   * @return a thrift server socket
   */
  private TServerSocket createThriftServerSocket() {
    try {
      return new TServerSocket(
          NetworkAddressUtils.getBindAddress(ServiceType.JOB_WORKER_RPC));
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }
}
