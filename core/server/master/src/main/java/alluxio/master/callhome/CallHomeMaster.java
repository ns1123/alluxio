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

package alluxio.master.callhome;

import alluxio.CallHomeConstants;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.clock.SystemClock;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.AlluxioMasterService;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.JournalFactory;
import alluxio.master.license.License;
import alluxio.master.license.LicenseMaster;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.NetworkInterface;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This service periodically collects call home information and stores it to Alluxio company's
 * backend.
 */
@ThreadSafe
public final class CallHomeMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(CallHomeMaster.class);
  private static final Set<Class<?>>
      DEPS = ImmutableSet.<Class<?>>of(BlockMaster.class, LicenseMaster.class);
  private static final String TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"; // RFC3339

  /** Handle to the Alluxio master service for collecting call home information. */
  private AlluxioMasterService mMaster;

  /**
   * The service that performs license check.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mCallHomeService;

  /**
   * Creates a new instance of {@link CallHomeMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  public CallHomeMaster(MasterRegistry registry, JournalFactory journalFactory) {
    super(journalFactory.create(Constants.CALL_HOME_MASTER_NAME), new SystemClock(),
        ExecutorServiceFactories
            .fixedThreadPoolExecutorServiceFactory(Constants.CALL_HOME_MASTER_NAME, 2));
    registry.add(LicenseMaster.class, this);
  }

  /**
   * Sets the master to be used in {@link CallHomeExecutor} for collecting call home information.
   * This should be called before calling {@link #start(boolean)}.
   *
   * @param master the master to use
   */
  public void setMaster(AlluxioMasterService master) {
    mMaster = master;
  }

  @Override
  public Set<Class<?>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.CALL_HOME_MASTER_NAME;
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    Preconditions.checkNotNull(mMaster, "Handle to the Alluxio master service is not specified");
    if (!isLeader) {
      return;
    }
    LOG.info("Starting {}", getName());
    mCallHomeService = getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.MASTER_CALL_HOME, new CallHomeExecutor(mMaster),
            Long.parseLong(CallHomeConstants.CALL_HOME_PERIOD_MS)));
    LOG.info("{} is started", getName());
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return new HashMap<>();
  }

  @Override
  public void processJournalEntry(Journal.JournalEntry entry) throws IOException {
    // No journal.
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    // No Journal
    return CommonUtils.nullIterator();
  }

  /**
   * Collects and saves call home information during the heartbeat.
   */
  @ThreadSafe
  public static final class CallHomeExecutor implements HeartbeatExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(CallHomeExecutor.class);

    private AlluxioMasterService mMaster;
    private BlockMaster mBlockMaster;
    private LicenseMaster mLicenseMaster;

    /**
     * Creates a new instance of {@link CallHomeExecutor}.
     *
     * @param master the Alluxio master service to be used for collecting call home information
     */
    public CallHomeExecutor(AlluxioMasterService master) {
      mMaster = master;
      mBlockMaster = master.getMaster(BlockMaster.class);
      mLicenseMaster = master.getMaster(LicenseMaster.class);
    }

    @Override
    public void heartbeat() throws InterruptedException {
      if (!mMaster.isServing()) {
        // Collect call home information only when master is up and running.
        return;
      }
      CallHomeInfo info = null;
      try {
        info = collect();
      } catch (IOException e) {
        LOG.error("Failed to collect call home information: {}", e);
      }
      if (info == null) {
        return;
      }
      try {
        upload(info);
      } catch (IOException e) {
        // When clusters do not have internet connection, this error is expected, so the error is
        // only logged as warnings.
        LOG.warn("Failed to upload call home information: {}", e.getMessage());
      }
    }

    @Override
    public void close() {
      // Nothing to close.
    }

    /**
     * @return the collected call home information, null if license hasn't been loaded
     * @throws IOException when failed to collect call home information
     */
    private CallHomeInfo collect() throws IOException {
      License license = mLicenseMaster.getLicense();
      if (license.getKey() == null) {
        // License hasn't been loaded.
        return null;
      }
      CallHomeInfo info = new CallHomeInfo();
      info.setLicenseKey(license.getKey());
      info.setFaultTolerant(Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
      info.setWorkerCount(mBlockMaster.getWorkerCount());
      info.setStartTime(mMaster.getStartTimeMs());
      info.setUptime(mMaster.getUptimeMs());
      info.setClusterVersion(RuntimeConstants.VERSION);
      // Set ufs information.
      String ufsRoot = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
      UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsRoot);
      info.setUfsType(ufs.getUnderFSType());
      info.setUfsSize(ufs.getSpace(ufsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL));
      // Set storage tiers.
      List<String> aliases = mBlockMaster.getGlobalStorageTierAssoc()
          .getOrderedStorageAliases();
      Map<String, Long> tierSizes = mBlockMaster.getTotalBytesOnTiers();
      List<CallHomeInfo.StorageTier> tiers = Lists.newArrayList();
      for (String alias : aliases) {
        CallHomeInfo.StorageTier tier = new CallHomeInfo.StorageTier();
        if (tierSizes.containsKey(alias)) {
          tier.setAlias(alias);
          tier.setSize(tierSizes.get(alias));
          tiers.add(tier);
        }
      }
      info.setStorageTiers(tiers.toArray(new CallHomeInfo.StorageTier[tiers.size()]));
      return info;
    }

    /**
     * @return the MAC address of the network interface being used by the master
     * @throws IOException when no MAC address is found
     */
    private byte[] getMACAddress() throws IOException {
      // Try to get the MAC address of the network interface of the master's RPC address.
      NetworkInterface nic = NetworkInterface.getByInetAddress(
          mMaster.getRpcAddress().getAddress());
      byte[] mac = nic.getHardwareAddress();
      if (mac != null) {
        return mac;
      }

      // Try to get the MAC address of the common "en0" interface.
      nic = NetworkInterface.getByName("en0");
      mac = nic.getHardwareAddress();
      if (mac != null) {
        return mac;
      }

      // Try to get the first non-empty MAC address in the enumeration of all network interfaces.
      Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
      while (ifaces.hasMoreElements()) {
        nic = ifaces.nextElement();
        mac = nic.getHardwareAddress();
        if (mac != null) {
          return mac;
        }
      }

      throw new IOException("No MAC address was found");
    }

    /**
     * @return the object key of the call home information
     * @throws IOException when failed to construct the object key
     */
    private String getObjectKey(String licenseKey) throws IOException {
      // Get time related information.
      Date now = new Date(System.currentTimeMillis());
      DateFormat formatter = new SimpleDateFormat(TIME_FORMAT);
      String time = formatter.format(now);
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(now);
      int year = calendar.get(Calendar.YEAR);
      int month = calendar.get(Calendar.MONTH) + 1; // get(Calendar.MONTH) returns 0 for January
      int day = calendar.get(Calendar.DAY_OF_MONTH);

      // Get MAC address.
      StringBuilder sb = new StringBuilder(18);
      for (byte b : getMACAddress()) {
        if (sb.length() > 0) {
          sb.append(':');
        }
        sb.append(String.format("%02x", b));
      }
      String mac = sb.toString();

      Joiner joiner = Joiner.on("/");
      return joiner.join("user", year, month, day, licenseKey, mac + "-" + time);
    }

    /**
     * Uploads the collected call home information to Alluxio company's backend.
     *
     * @param info the call home information to be uploaded
     */
    private void upload(CallHomeInfo info) throws IOException {
      // Encode info into json as payload.
      String payload = "";
      try {
        ObjectMapper mapper = new ObjectMapper();
        payload = mapper.writeValueAsString(info);
      } catch (JsonProcessingException e) {
        throw new IOException("Failed to encode CallHomeInfo as json: " + e);
      }

      // Get license.
      License license = mLicenseMaster.getLicense();
      if (license == null) {
        throw new IOException("License not found");
      }
      if (!license.getKey().equals(info.getLicenseKey())) {
        throw new IOException("Inconsistent license key");
      }

      // Create the upload request.
      String objectKey = getObjectKey(info.getLicenseKey());
      Joiner joiner = Joiner.on("/");
      String path = joiner.join("upload", CallHomeConstants.CALL_HOME_BUCKET, objectKey);
      String url = new URL(new URL(ProjectConstants.PROXY_URL), path).toString();
      HttpPost post = new HttpPost(url);
      MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      builder.addBinaryBody("payload", payload.getBytes(), ContentType.APPLICATION_OCTET_STREAM,
          "payload");
      builder.addTextBody("email", license.getEmail());
      try {
        builder.addTextBody("token", license.getToken());
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
      post.setEntity(builder.build());

      // Fire off the upload request.
      HttpClient client = HttpClientBuilder.create().build();
      HttpResponse response = client.execute(post);

      // Check the response code.
      int responseCode = response.getStatusLine().getStatusCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException("Call home upload request failed with: " + responseCode);
      }
    }
  }
}
