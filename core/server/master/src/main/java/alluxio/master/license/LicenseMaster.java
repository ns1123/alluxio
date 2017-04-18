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

package alluxio.master.license;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LicenseConstants;
import alluxio.ProjectConstants;
import alluxio.PropertyKey;
import alluxio.clock.SystemClock;
import alluxio.exception.ExceptionMessage;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.JournalFactory;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.thrift.TProcessor;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This master performs periodic license check.
 */
public class LicenseMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(LicenseMaster.class);
  private static final Set<Class<?>> DEPS = ImmutableSet.<Class<?>>of(BlockMaster.class);

  private BlockMaster mBlockMaster;
  private final LicenseCheck mLicenseCheck;
  private License mLicense;

  /**
   * The service that performs license check.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLicenseCheckService;

  /**
   * Creates a new instance of {@link LicenseMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  public LicenseMaster(MasterRegistry registry, JournalFactory journalFactory) {
    super(journalFactory.create(Constants.LICENSE_MASTER_NAME), new SystemClock(),
        ExecutorServiceFactories
            .fixedThreadPoolExecutorServiceFactory(Constants.LICENSE_MASTER_NAME, 2));
    mBlockMaster = registry.get(BlockMaster.class);
    mLicenseCheck = new LicenseCheck();
    mLicense = new License();
    registry.add(LicenseMaster.class, this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return new HashMap<>();
  }

  @Override
  public Set<Class<?>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.LICENSE_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(alluxio.proto.journal.Journal.JournalEntry entry)
      throws IOException {
    if (entry.hasLicenseCheck()) {
      long timeMs = entry.getLicenseCheck().getTimeMs();
      mLicenseCheck.setLastCheck(timeMs);
      mLicenseCheck.setLastCheckSuccess(timeMs);
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
    if (!isLeader) {
      return;
    }
    LOG.info("Starting {}", getName());
    mLicenseCheckService = getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.MASTER_LICENSE_CHECK,
            new LicenseCheckExecutor(mBlockMaster, this),
            Long.parseLong(LicenseConstants.LICENSE_CHECK_PERIOD_MS)));
    LOG.info("{} is started", getName());
  }

  @Override
  public synchronized Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    return CommonUtils.singleElementIterator(mLicenseCheck.toJournalEntry());
  }

  /**
   * @return the license
   */
  public License getLicense() {
    return mLicense;
  }

  /**
   * @return the license check status
   */
  public LicenseCheck getLicenseCheck() {
    return mLicenseCheck;
  }

  /**
   * @param license the license to use
   */
  public void setLicense(License license) {
    mLicense = license;
  }

  /**
   * @param blockMaster the block master to set for the license master
   */
  // This method is a temporary hack to fix the license master in branch enterprise-1.3. A proper
  // fix will be merged to master, so we should never merge this to master.
  public void setBlockMaster(BlockMaster blockMaster) {
    mBlockMaster = blockMaster;
    mBlockMaster.setMaxWorkers(getLicense().getNodes());
  }

  /**
   * Performs the license check.
   */
  @NotThreadSafe
  public static final class LicenseCheckExecutor implements HeartbeatExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(LicenseCheckExecutor.class);

    BlockMaster mBlockMaster;
    LicenseMaster mLicenseMaster;

    /**
     * Creates a new instance of {@link LicenseCheckExecutor}.
     *
     * @param blockMaster the block master
     * @param licenseMaster the license master
     */
    public LicenseCheckExecutor(BlockMaster blockMaster, LicenseMaster licenseMaster) {
      mBlockMaster = blockMaster;
      mLicenseMaster = licenseMaster;
    }

    /**
     * Reads the license file.
     *
     * @return a license (if the license file exists and can be parsed) or null
     */
    private License readLicense() {
      String licenseFilePath = Configuration.get(PropertyKey.LICENSE_FILE);
      License license;
      try {
        ObjectMapper mapper = new ObjectMapper();
        license = mapper.readValue(new File(licenseFilePath), License.class);
      } catch (IOException e) {
        LOG.error("Failed to parse license file {}: {}", licenseFilePath, e);
        return null;
      }
      return license;
    }

    private boolean remoteCheck(License license) {
      // If remote check is disabled, return immediately.
      if (!license.getRemote()) {
        return true;
      }

      // Decrypt access token from the license.
      String token;
      try {
        token = license.getToken();
      } catch (GeneralSecurityException | IOException e) {
        LOG.error("Failed to decrypt license secret: {}", e);
        return false;
      }

      // Create the remote check request.
      String url = "";
      try {
        url = new URL(new URL(ProjectConstants.PROXY_URL), "check").toString();
      } catch (MalformedURLException e) {
        LOG.error("Failed to construct URL to the license check server: {}", e);
        return false;
      }
      HttpClient client = HttpClientBuilder.create().build();
      HttpPost post = new HttpPost(url);
      List<NameValuePair> urlParameters = new ArrayList<>();
      urlParameters.add(new BasicNameValuePair("key", license.getKey()));
      urlParameters.add(new BasicNameValuePair("email", license.getEmail()));
      urlParameters.add(new BasicNameValuePair("token", token));
      try {
        post.setEntity(new UrlEncodedFormEntity(urlParameters));
      } catch (UnsupportedEncodingException e) {
        LOG.error("Failed to set request entity: {}", e);
        return false;
      }

      // Fire off the remote check request.
      HttpResponse response;
      try {
        response = client.execute(post);
      } catch (IOException e) {
        LOG.error("Failed to execute the request: {}", e);
        return false;
      }

      // Check the response code.
      int responseCode = response.getStatusLine().getStatusCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        LOG.error("Remote check request failed with: {}", responseCode);
        return false;
      }

      // Read the response.
      StringBuilder result = new StringBuilder();
      try (BufferedReader rd = new BufferedReader(
          new InputStreamReader(response.getEntity().getContent()))) {
        String inputLine;
        while ((inputLine = rd.readLine()) != null) {
          result.append(inputLine);
        }
      } catch (IOException e) {
        LOG.error("Failed to read response content: {}", e);
        return false;
      }

      return Boolean.parseBoolean(result.toString());
    }

    /**
     * @return whether the given license is valid
     */
    private boolean checkLicense(License license) {
      if (!license.isValid()) {
        LOG.error("Failed to validate license checksum");
        return false;
      }

      long currentTimeMs = CommonUtils.getCurrentMs();
      long expirationTimeMs;
      try {
        expirationTimeMs = license.getExpirationMs();
      } catch (ParseException e) {
        LOG.error("Failed to parse expiration {}: {}", license.getExpiration(), e);
        return false;
      }
      if (currentTimeMs > expirationTimeMs) {
        // License is expired.
        LOG.error("The license has expired on {}", license.getExpiration());
        return false;
      }

      return remoteCheck(license);
    }

    @Override
    public void heartbeat() {
      License license = readLicense();
      if (license == null) {
        LOG.error("The license file is missing; the cluster will shut down now.");
        System.exit(-1);
      }
      boolean isValid = checkLicense(license);
      long currentTimeMs = CommonUtils.getCurrentMs();
      mLicenseMaster.mLicenseCheck.setLastCheck(currentTimeMs);
      mLicenseMaster.setLicense(license);
      if (isValid) {
        // Set the maximum number of workers.
        mBlockMaster.setMaxWorkers(license.getNodes());
        LOG.info("The license check succeeded.");
        mLicenseMaster.mLicenseCheck.setLastCheckSuccess(currentTimeMs);
        mLicenseMaster.writeJournalEntry(mLicenseMaster.mLicenseCheck.toJournalEntry());
      } else {
        // The license check failed.
        long lastSuccessMs = mLicenseMaster.mLicenseCheck.getLastCheckSuccessMs();
        long gracePeriodEndMs = mLicenseMaster.mLicenseCheck.getGracePeriodEndMs();
        if (lastSuccessMs == 0) {
          LOG.error("The initial license check failed; the cluster will shut down now.");
          System.exit(-1);
        } else if (currentTimeMs > gracePeriodEndMs) {
          LOG.error("The license check failed and the grace period ended on {}. The cluster will "
              + " shut down now.", mLicenseMaster.mLicenseCheck.getGracePeriodEnd());
          System.exit(-1);
        } else {
          LOG.warn("The license check failed. If the license check does not succeed again by {}, "
                  + "the cluster will shut down at that point.",
              mLicenseMaster.mLicenseCheck.getGracePeriodEnd());
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
