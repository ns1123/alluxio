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

import alluxio.Constants;
import alluxio.LicenseConstants;
import alluxio.master.Master;
import alluxio.master.MasterFactory;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link LicenseMaster} instance.
 */
@ThreadSafe
public final class LicenseMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LicenseMasterFactory.class);

  /**
   * Constructs a new {@link LicenseMasterFactory}.
   */
  public LicenseMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return Boolean.parseBoolean(LicenseConstants.LICENSE_CHECK_ENABLED);
  }

  @Override
  public String getName() {
    return Constants.LICENSE_MASTER_NAME;
  }

  @Override
  public LicenseMaster create(List<? extends Master> masters, JournalFactory journalFactory) {
    if (!isEnabled()) {
      return null;
    }
    Preconditions.checkArgument(journalFactory != null, "journal factory may not be null");
    LOG.info("Creating {} ", LicenseMaster.class.getName());

    for (Master master : masters) {
      if (master instanceof BlockMaster) {
        LOG.info("{} is created", LicenseMaster.class.getName());
        Journal journal = journalFactory.create(Constants.LICENSE_MASTER_NAME);
        return new LicenseMaster((BlockMaster) master, journal);
      }
    }
    LOG.error("Fail to create {} due to missing {}", LicenseMaster.class.getName(),
        BlockMaster.class.getName());
    return null;
  }
}
