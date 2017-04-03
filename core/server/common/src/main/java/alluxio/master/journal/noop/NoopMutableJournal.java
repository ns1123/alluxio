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

import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.MutableJournal;

import java.io.IOException;

/**
 * Implementation of {@link MutableJournal} that does nothing.
 */
public class NoopMutableJournal extends NoopJournal implements MutableJournal {

  /**
   * Creates a new instance of {@link NoopMutableJournal}.
   */
  public NoopMutableJournal() {}

  @Override
  public void format() throws IOException {}

  /**
   * @return the {@link JournalWriter} for this journal
   */
  public JournalWriter getWriter() {
    return new NoopJournalWriter();
  }
}
