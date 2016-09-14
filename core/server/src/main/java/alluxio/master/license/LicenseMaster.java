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
import alluxio.exception.ExceptionMessage;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.master.journal.JournalProtoUtils;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.thrift.TProcessor;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This master performs periodic license check.
 */
public class LicenseMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final BlockMaster mBlockMaster;
  private final LicenseCheck mLicenseCheck;
  private License mLicense;

  /**
   * The service that performs license check.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLicenseCheckService;

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.LICENSE_MASTER_NAME);
  }

  /**
   * Creates a new instance of {@link LicenseMaster}.
   *
   * @param blockMaster the block master
   * @param journal the journal
   */
  public LicenseMaster(BlockMaster blockMaster, Journal journal) {
    super(journal, 2);
    mBlockMaster = blockMaster;
    mLicenseCheck = new LicenseCheck();
    mLicense = new License();
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return new HashMap<>();
  }

  @Override
  public String getName() {
    return Constants.LICENSE_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(alluxio.proto.journal.Journal.JournalEntry entry)
      throws IOException {
    Message innerEntry = JournalProtoUtils.unwrap(entry);
    if (innerEntry instanceof alluxio.proto.journal.License.LicenseCheckEntry) {
      long timeMs = ((alluxio.proto.journal.License.LicenseCheckEntry) innerEntry).getTimeMs();
      mLicenseCheck.setLast(timeMs);
      mLicenseCheck.setLastSuccess(timeMs);
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(innerEntry));
    }
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      mLicenseCheckService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_LICENSE_CHECK,
              new LicenseCheckExecutor(mBlockMaster, this), Constants.MINUTE_MS));
    }
  }

  @Override
  public synchronized void streamToJournalCheckpoint(JournalOutputStream outputStream)
      throws IOException {
    outputStream.writeEntry(mLicenseCheck.toJournalEntry());
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
   * Performs the license check.
   */
  @NotThreadSafe
  public static final class LicenseCheckExecutor implements HeartbeatExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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
     * Performs various license checks.
     *
     * @return a valid license (if all checks are successful) or null
     */
    private License check() {
      String licenseFilePath = Configuration.get(Constants.LICENSE_FILE);
      License license;
      try {
        ObjectMapper mapper = new ObjectMapper();
        license = mapper.readValue(new File(licenseFilePath), License.class);
      } catch (IOException e) {
        LOG.error("Failed to parse license file {}: {}", licenseFilePath, e);
        return null;
      }

      if (!license.isValid()) {
        LOG.error("Failed to validate license checksum");
        return null;
      }

      // Set the maximum number of workers.
      mBlockMaster.setMaxWorkers(license.getNodes());

      long currentTimeMs = CommonUtils.getCurrentMs();
      long expirationTimeMs;
      try {
        expirationTimeMs = license.getExpirationMs();
      } catch (ParseException e) {
        LOG.error("Failed to parse expiration {}: {}", license.getExpiration(), e);
        return null;
      }
      if (currentTimeMs > expirationTimeMs) {
        // License is expired.
        LOG.error("The license has expired on {}", license.getExpiration());
        return null;
      }

      if (license.getRemote()) {
        String token;
        try {
          token = license.getToken();
        } catch (GeneralSecurityException | IOException e) {
          LOG.error("Failed to decrypt license secret: {}", e);
          return null;
        }
        // TODO(jiri): perform remote check
      }

      return license;
    }

    @Override
    public void heartbeat() {
      License license = check();
      mLicenseMaster.mLicenseCheck.setLast(CommonUtils.getCurrentMs());
      if (license != null) {
        // The license check succeeded.
        LOG.info("The license check succeeded.");
        mLicenseMaster.setLicense(license);
        mLicenseMaster.mLicenseCheck.setLastSuccess(CommonUtils.getCurrentMs());
        mLicenseMaster.writeJournalEntry(mLicenseMaster.mLicenseCheck.toJournalEntry());
      } else {
        // The license check failed.
        long currentTimeMs = CommonUtils.getCurrentMs();
        long lastSuccessMs = mLicenseMaster.mLicenseCheck.getLastSuccessMs();
        long gracePeriodEndMs = lastSuccessMs
            + Long.parseLong(LicenseConstants.LICENSE_GRACE_PERIOD) * Constants.DAY_MS;
        Date date = new Date(gracePeriodEndMs);
        DateFormat formatter = new SimpleDateFormat(License.TIME_FORMAT);
        if (lastSuccessMs == 0) {
          LOG.error("The initial license check failed; the cluster will shut down now.");
          System.exit(-1);
        } else if (currentTimeMs > gracePeriodEndMs) {
          LOG.error("The license check failed and the grace period ended. The "
              + "cluster will shut down now.", LicenseConstants.LICENSE_GRACE_PERIOD);
          System.exit(-1);
        } else {
          LOG.warn("The license check failed. Unless the license check succeeds before {}, the "
              + "cluster will shut down at the point.", formatter.format(date));
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
