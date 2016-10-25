/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.proto.journal.Journal.JournalEntry;

/**
 * An interface for a class which writes journal entries.
 */
public interface JournalEntryWriter {

  /**
   * Writes an entry to the checkpoint file.
   *
   * The entry should not have its sequence number set. This method will add the proper sequence
   * number to the passed in entry.
   *
   * @param entry an entry to write to the journal checkpoint file
   */
  void writeJournalEntry(JournalEntry entry);
}
