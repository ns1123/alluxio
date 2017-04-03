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

package alluxio.master.journal.noop;

import alluxio.master.journal.JournalOutputStream;
import alluxio.master.journal.JournalWriter;
import alluxio.proto.journal.Journal;

import java.io.IOException;

/**
 * Implementation of {@link JournalWriter} that does nothing.
 */
public class NoopJournalWriter implements JournalWriter {

  /**
   * Creates a new instance of {@link NoopJournalWriter}.
   */
  public NoopJournalWriter() {}

  @Override
  public void completeLogs() throws IOException {}

  @Override
  public JournalOutputStream getCheckpointOutputStream(long latestSequenceNumber)
      throws IOException {
    return new NoopJournalOutputStream();
  }

  @Override
  public void write(Journal.JournalEntry entry) throws IOException {}

  @Override
  public void flush() throws IOException {}

  @Override
  public long getNextSequenceNumber() {
    return 0;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void recover() {}

  @Override
  public void deleteCompletedLogs() throws IOException {}

  @Override
  public void completeCurrentLog() throws IOException {}
}
