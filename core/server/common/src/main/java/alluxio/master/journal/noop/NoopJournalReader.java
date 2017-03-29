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
import alluxio.master.journal.JournalReader;

import java.io.IOException;

/**
 * Implementation of {@link JournalReader} that does nothing.
 */
public class NoopJournalReader implements JournalReader {

  /**
   * Creates a new instance of {@link NoopJournalReader}.
   */
  public NoopJournalReader() {}

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public JournalInputStream getCheckpointInputStream() throws IOException {
    return new NoopJournalInputStream();
  }

  @Override
  public JournalInputStream getNextInputStream() throws IOException {
    return new NoopJournalInputStream();
  }

  @Override
  public long getCheckpointLastModifiedTimeMs() throws IOException {
    return 0;
  }
}
