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

import alluxio.master.journal.JournalInputStream;
import alluxio.proto.journal.Journal;

import java.io.IOException;

/**
 * Implementation of {@link JournalInputStream} that does nothing.
 */
public class NoopJournalInputStream implements JournalInputStream {

  /**
   * Creates a new instance of {@link NoopJournalInputStream}.
   */
  public NoopJournalInputStream() {}

  @Override
  public Journal.JournalEntry read() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public long getLatestSequenceNumber() {
    return 0;
  }
}
