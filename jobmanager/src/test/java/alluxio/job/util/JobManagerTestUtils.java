/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.util;

import alluxio.master.job.command.CommandManager;

import jersey.repackaged.com.google.common.collect.Maps;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * A utility class for job manager tests.
 */
public final class JobManagerTestUtils {
  private JobManagerTestUtils() {} // prevent instantiation

  public static void cleanUpCommandManager() {
    CommandManager manager = CommandManager.INSTANCE;
    // clear up the state
    Whitebox.setInternalState(manager, "mWorkerIdToPendingCommands", Maps.newHashMap());
  }
}
