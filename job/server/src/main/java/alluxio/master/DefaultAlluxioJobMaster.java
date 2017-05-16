/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.concurrent.Executors;
import alluxio.master.job.JobMaster;
import alluxio.master.job.JobMasterClientServiceHandler;
import alluxio.master.journal.JournalFactory;
import alluxio.security.authentication.AuthenticatedThriftServer;
import alluxio.security.authentication.TransportProvider;
import alluxio.thrift.JobMasterClientService;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.JobMasterWebServer;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
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
 * This class is responsible for initializing the different masters that are configured to run.
 */
@NotThreadSafe
public class DefaultAlluxioJobMaster implements AlluxioJobMasterService {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAlluxioJobMaster.class);

  /** Maximum number of threads to serve the rpc server. */
  private final int mMaxWorkerThreads;

  /** Minimum number of threads to serve the rpc server. */
  private final int mMinWorkerThreads;

  /** The port for the RPC server. */
  private final int mPort;

  /** The socket for thrift rpc server. */
  private final TServerSocket mTServerSocket;

  /** The transport provider to create thrift server transport. */
  private final TransportProvider mTransportProvider;

  /** The address for the rpc server. */
  private final InetSocketAddress mRpcAddress;

  /** The master managing all job related metadata. */
  protected JobMaster mJobMaster;

  /** The RPC server. */
  private AuthenticatedThriftServer mMasterServiceServer = null;

  /** is true if the master is serving the RPC server. */
  private boolean mIsServing = false;

  /** The start time for when the master started serving the RPC server. */
  private long mStartTimeMs = -1;

  /** The master services' names. */
  private static List<String> sServiceNames;

  /** The master service loaders. */
  private static ServiceLoader<MasterFactory> sServiceLoader;

  /** The web ui server. */
  private JobMasterWebServer mWebServer = null;

  /**
   * @return the (cached) master service loader
   */
  private static ServiceLoader<MasterFactory> getServiceLoader() {
    if (sServiceLoader != null) {
      return sServiceLoader;
    }
    // Discover and register the available factories.
    // NOTE: ClassLoader is explicitly specified so we don't need to set ContextClassLoader.
    sServiceLoader = ServiceLoader.load(MasterFactory.class, MasterFactory.class.getClassLoader());
    return sServiceLoader;
  }

  /**
   * @return the (cached) list of the enabled master services' names
   */
  public static List<String> getServiceNames() {
    if (sServiceNames != null) {
      return sServiceNames;
    }
    sServiceNames = Lists.newArrayList();
    sServiceNames.add(Constants.JOB_MASTER_NAME);

    return sServiceNames;
  }

  protected DefaultAlluxioJobMaster() {
    mMinWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN);
    mMaxWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX);

    Preconditions.checkArgument(mMaxWorkerThreads >= mMinWorkerThreads,
        PropertyKey.MASTER_WORKER_THREADS_MAX + " can not be less than "
            + PropertyKey.MASTER_WORKER_THREADS_MIN);

    try {
      // Extract the port from the generated socket.
      // When running tests, it is fine to use port '0' so the system will figure out what port to
      // use (any random free port).
      // In a production or any real deployment setup, port '0' should not be used as it will make
      // deployment more complicated.
      if (!Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        Preconditions.checkState(Configuration.getInt(PropertyKey.JOB_MASTER_RPC_PORT) > 0,
            "Master rpc port is only allowed to be zero in test mode.");
        Preconditions.checkState(Configuration.getInt(PropertyKey.JOB_MASTER_WEB_PORT) > 0,
            "Master web port is only allowed to be zero in test mode.");
      }
      mTransportProvider = TransportProvider.Factory.create();
      mTServerSocket = new TServerSocket(
          NetworkAddressUtils.getBindAddress(ServiceType.JOB_MASTER_RPC));
      mPort = NetworkAddressUtils.getThriftPort(mTServerSocket);
      // reset master port
      Configuration.set(PropertyKey.JOB_MASTER_RPC_PORT, Integer.toString(mPort));
      mRpcAddress =
          NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RPC);

      createMasters(new JournalFactory.ReadWrite(getJournalDirectory()));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  protected String getJournalDirectory() {
    String journalDirectory = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
      journalDirectory += AlluxioURI.SEPARATOR;
    }
    // Preconditions.checkState(isJournalFormatted(journalDirectory),
    //     "Alluxio was not formatted! The journal folder is " + journalDirectory);
    return journalDirectory;
  }

  /**
   * @param journalFactory the factory to use for creating journals
   */
  protected void createMasters(JournalFactory journalFactory) {
    mJobMaster = new JobMaster(journalFactory);
  }

  @Override
  public JobMaster getJobMaster() {
    return mJobMaster;
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
  public boolean isServing() {
    return mIsServing;
  }

  @Override
  public InetSocketAddress getWebAddress() {
    if (mWebServer != null) {
      return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
    }
    return null;
  }

  @Override
  public void waitForReady() {
    CommonUtils.waitFor("Alluxio job master to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mMasterServiceServer != null && mMasterServiceServer.isServing()
            && mWebServer != null && mWebServer.getServer().isRunning();
      }
    });
  }

  /**
   * Starts the Alluxio job master server.
   *
   * @throws Exception if starting the master fails
   */
  public void start() throws Exception {
    startMasters(true);
    startServing();
  }

  /**
   * Stops the Alluxio job master server.
   *
   * @throws Exception if stopping the master fails
   */
  public void stop() throws Exception {
    LOG.info("Stopping RPC server on Alluxio Job Master @ {}", mRpcAddress);
    if (mIsServing) {
      stopServing();
      stopMasters();
      mTServerSocket.close();
      mIsServing = false;
    }
  }

  protected void startMasters(boolean isLeader) {
    try {
      mJobMaster.start(isLeader);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  protected void stopMasters() {
    try {
      mJobMaster.stop();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  private void startServing() {
    startServing("", "");
  }

  protected void startServing(String startMessage, String stopMessage) {
    startServingWebServer();
    LOG.info("Alluxio Job Master version {} started @ {} {}", RuntimeConstants.VERSION, mRpcAddress,
        startMessage);
    startServingRPCServer();
    LOG.info("Alluxio Job Master version {} ended @ {} {}", RuntimeConstants.VERSION, mRpcAddress,
        stopMessage);
  }

  protected void startServingWebServer() {
    mWebServer = new JobMasterWebServer(ServiceType.JOB_MASTER_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.JOB_MASTER_WEB), this);
    // reset master web port
    Configuration.set(PropertyKey.JOB_MASTER_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
    mWebServer.start();
  }

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
      LOG.info("registered service {}", service.getKey());
    }
  }

  protected void startServingRPCServer() {
    // set up multiplexed thrift processors
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    registerServices(processor, mJobMaster.getServices());
    // register meta services
    processor.registerProcessor(Constants.JOB_MASTER_CLIENT_SERVICE_NAME,
        new JobMasterClientService.Processor<>(new JobMasterClientServiceHandler(mJobMaster)));
    LOG.info("registered service " + Constants.JOB_MASTER_CLIENT_SERVICE_NAME);

    // Return a TTransportFactory based on the authentication type
    TTransportFactory transportFactory;
    try {
      String serverName = NetworkAddressUtils.getConnectHost(ServiceType.JOB_MASTER_RPC);
      transportFactory = mTransportProvider.getServerTransportFactory(serverName);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // create master thrift service with the multiplexed processor.
    Args args = new Args(mTServerSocket).maxWorkerThreads(mMaxWorkerThreads)
        .minWorkerThreads(mMinWorkerThreads).processor(processor).transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true));
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      args.stopTimeoutVal = 0;
    } else {
      args.stopTimeoutVal = Constants.THRIFT_STOP_TIMEOUT_SECONDS;
    }
    args.executorService(Executors.createDefaultExecutorServiceWithSecurityOn(args));
    mMasterServiceServer = new AuthenticatedThriftServer(args);

    // start thrift rpc server
    mIsServing = true;
    mStartTimeMs = System.currentTimeMillis();
    mMasterServiceServer.serve();
  }

  protected void stopServing() throws Exception {
    if (mMasterServiceServer != null) {
      mMasterServiceServer.stop();
      mMasterServiceServer = null;
    }
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
    mIsServing = false;
  }

  /**
   * Checks to see if the journal directory is formatted.
   *
   * @param journalDirectory The journal directory to check
   * @return true if the journal directory was formatted previously, false otherwise
   * @throws IOException if an I/O error occurs
   */
  private boolean isJournalFormatted(String journalDirectory) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.get(journalDirectory);
    UnderFileStatus[] files = ufs.listStatus(journalDirectory);
    if (files == null) {
      return false;
    }
    // Search for the format file.
    String formatFilePrefix = Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX);
    for (UnderFileStatus file : files) {
      if (file.getName().startsWith(formatFilePrefix)) {
        return true;
      }
    }
    return false;
  }
}
