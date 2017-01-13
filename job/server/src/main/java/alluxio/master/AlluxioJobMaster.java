/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
<<<<<<< HEAD
import alluxio.ServerUtils;
import alluxio.util.ConfigurationUtils;
||||||| merged common ancestors
import alluxio.concurrent.Executors;
import alluxio.master.job.JobMaster;
import alluxio.master.job.MetaJobMasterClientServiceHandler;
import alluxio.master.journal.JournalFactory;
import alluxio.security.authentication.AuthenticatedThriftServer;
import alluxio.security.authentication.TransportProvider;
import alluxio.thrift.MetaJobMasterClientService;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.JobMasterWebServer;
=======
import alluxio.concurrent.Executors;
import alluxio.master.job.JobMaster;
import alluxio.master.job.JobMasterClientServiceHandler;
import alluxio.master.journal.JournalFactory;
import alluxio.security.authentication.AuthenticatedThriftServer;
import alluxio.security.authentication.TransportProvider;
import alluxio.thrift.JobMasterClientService;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.JobMasterWebServer;
>>>>>>> enterprise-1.4

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Entry point for the Alluxio job master program.
 */
@ThreadSafe
public final class AlluxioJobMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Starts the Alluxio job master.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioJobMaster.class.getCanonicalName());
      System.exit(-1);
    }

    if (!ConfigurationUtils.masterHostConfigured()) {
      System.out.println(String.format(
          "Cannot run alluxio job worker; master hostname is not "
              + "configured. Please modify %s to either set %s or configure zookeeper with "
              + "%s=true and %s=[comma-separated zookeeper master addresses]",
          Configuration.SITE_PROPERTIES, PropertyKey.MASTER_HOSTNAME.toString(),
          PropertyKey.ZOOKEEPER_ENABLED.toString(), PropertyKey.ZOOKEEPER_ADDRESS.toString()));
      System.exit(1);
    }

<<<<<<< HEAD
    if (!ConfigurationUtils.jobMasterHostConfigured()) {
      System.out.println(String.format(
          "Cannot run alluxio job worker; job master hostname is not "
              + "configured. Please modify %s to either set %s or configure zookeeper with "
              + "%s=true and %s=[comma-separated zookeeper master addresses]",
          Configuration.SITE_PROPERTIES, PropertyKey.JOB_MASTER_HOSTNAME.toString(),
          PropertyKey.ZOOKEEPER_ENABLED.toString(), PropertyKey.ZOOKEEPER_ADDRESS.toString()));
      System.exit(1);
||||||| merged common ancestors
  /**
   * @return the start time of the master in milliseconds
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the uptime of the master in milliseconds
   */
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing() {
    return mIsServing;
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
    if (mIsServing) {
      LOG.info("Stopping RPC server on Alluxio Job Master @ {}", mMasterAddress);
      stopServing();
      stopMasters();
      mTServerSocket.close();
      mIsServing = false;
    } else {
      LOG.info("Stopping Alluxio Job aster @ {}", mMasterAddress);
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
    LOG.info("Alluxio Job Master version {} started @ {} {}", RuntimeConstants.VERSION,
        mMasterAddress, startMessage);
    startServingRPCServer();
    LOG.info("Alluxio Job Master version {} ended @ {} {}", RuntimeConstants.VERSION,
        mMasterAddress, stopMessage);
  }

  protected void startServingWebServer() {
    mWebServer = new JobMasterWebServer(ServiceType.JOB_MASTER_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.JOB_MASTER_WEB));
    // reset master web port
    Configuration.set(PropertyKey.JOB_MASTER_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
    mWebServer.start();
  }

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
      LOG.info("registered service {}", service.getKey());
=======
  /**
   * @return the start time of the master in milliseconds
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the uptime of the master in milliseconds
   */
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing() {
    return mIsServing;
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
    if (mIsServing) {
      LOG.info("Stopping RPC server on Alluxio Job Master @ {}", mMasterAddress);
      stopServing();
      stopMasters();
      mTServerSocket.close();
      mIsServing = false;
    } else {
      LOG.info("Stopping Alluxio Job aster @ {}", mMasterAddress);
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
    LOG.info("Alluxio Job Master version {} started @ {} {}", RuntimeConstants.VERSION,
        mMasterAddress, startMessage);
    startServingRPCServer();
    LOG.info("Alluxio Job Master version {} ended @ {} {}", RuntimeConstants.VERSION,
        mMasterAddress, stopMessage);
  }

  protected void startServingWebServer() {
    mWebServer = new JobMasterWebServer(ServiceType.JOB_MASTER_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.JOB_MASTER_WEB), mJobMaster);
    // reset master web port
    Configuration.set(PropertyKey.JOB_MASTER_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
    mWebServer.start();
  }

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
      LOG.info("registered service {}", service.getKey());
>>>>>>> enterprise-1.4
    }
<<<<<<< HEAD
||||||| merged common ancestors
  }

  protected void startServingRPCServer() {
    // set up multiplexed thrift processors
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    registerServices(processor, mJobMaster.getServices());
    // register meta services
    processor.registerProcessor(Constants.META_JOB_MASTER_CLIENT_SERVICE_NAME,
        new MetaJobMasterClientService.Processor<>(
            new MetaJobMasterClientServiceHandler(this)));
    LOG.info("registered service " + Constants.META_JOB_MASTER_CLIENT_SERVICE_NAME);

    // Return a TTransportFactory based on the authentication type
    TTransportFactory transportFactory;
    try {
      transportFactory = mTransportProvider.getServerTransportFactory();
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
=======
  }

  protected void startServingRPCServer() {
    // set up multiplexed thrift processors
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    registerServices(processor, mJobMaster.getServices());
    processor.registerProcessor(Constants.JOB_MASTER_CLIENT_SERVICE_NAME,
        new JobMasterClientService.Processor<>(new JobMasterClientServiceHandler(mJobMaster)));
    LOG.info("registered service " + Constants.JOB_MASTER_CLIENT_SERVICE_NAME);

    // Return a TTransportFactory based on the authentication type
    TTransportFactory transportFactory;
    try {
      transportFactory = mTransportProvider.getServerTransportFactory();
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
>>>>>>> enterprise-1.4

    AlluxioJobMasterService master = AlluxioJobMasterService.Factory.create();
    ServerUtils.run(master, "Alluxio job master");
  }

  private AlluxioJobMaster() {} // prevent instantiation
}
