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

package alluxio.master.privilege;

import alluxio.Constants;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;
import alluxio.master.journal.JournalFactory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link PrivilegeMaster} instance.
 */
@ThreadSafe
public final class PrivilegeMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(PrivilegeMasterFactory.class);

  /**
   * Constructs a new {@link PrivilegeMasterFactory}.
   */
  public PrivilegeMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return Constants.PRIVILEGE_MASTER_NAME;
  }

  @Override
  public PrivilegeMaster create(MasterRegistry registry, JournalFactory journalFactory) {
    Preconditions.checkArgument(journalFactory != null, "journal factory may not be null");
    LOG.info("Creating {} ", DefaultPrivilegeMaster.class.getName());
    PrivilegeMaster privilegeMaster = new DefaultPrivilegeMaster(journalFactory);
    registry.add(PrivilegeMaster.class, privilegeMaster);
    return privilegeMaster;
  }
}
