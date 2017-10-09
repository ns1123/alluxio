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

package alluxio.raft;

import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Abstract class for journal entry based state machines.
 */
public abstract class AbstractRaftStateMachine extends StateMachine implements Snapshottable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRaftStateMachine.class);

  private long mPreviousSequenceNumber = -1;

  /**
   * Resets all state machine state.
   */
  protected abstract void resetState();

  /**
   * Applies a journal entry to the state machine.
   *
   * @param entry the entry to apply
   */
  protected abstract void applyJournalEntry(JournalEntry entry);

  /**
   * Applies a journal entry commit to the state machine.
   *
   * @param commit the commit
   */
  public void applyJournalEntryCommand(Commit<JournalEntryCommand> commit) {
    JournalEntry entry;
    try {
      entry = JournalEntry.parseFrom(commit.command().getSerializedJournalEntry());
    } catch (Exception e) {
      LOG.error("Encountered invalid journal entry in commit: {}. Terminating process to prevent "
          + "inconsistency", commit, e);
      System.exit(-1);
      throw new IllegalStateException(e); // We should never reach here.
    }
    try {
      applyEntryInternal(entry);
    } finally {
      commit.release();
    }
  }

  /**
   * Applies the journal entry to the subclass, handling empty entries and multi-entries so that the
   * subclass only has to deal with normal entries.
   *
   * @param entry the entry to apply
   */
  private void applyEntryInternal(JournalEntry entry) {
    Preconditions.checkState(
        entry.getAllFields().size() <= 1
            || (entry.getAllFields().size() == 2 && entry.hasSequenceNumber()),
        "Raft journal entries should never set multiple fields in addition to sequence "
            + "number, but found %s",
        entry);
    if (entry.getJournalEntriesCount() > 0) {
      // This entry aggregates multiple entries.
      for (JournalEntry e : entry.getJournalEntriesList()) {
        applyEntryInternal(e);
      }
    } else if (entry.equals(JournalEntry.getDefaultInstance())) {
      // Ignore empty entries, they are created during snapshotting.
    } else {
      if (!entry.hasSequenceNumber()) {
        // Snapshots don't use sequence numbers, and can't have duplicate entries.
        applyJournalEntry(entry);
        return;
      }
      long sequenceNumber = entry.getSequenceNumber();
      // Sequence number restarts at 0 whenever a new master starts writing entries. Otherwise, if
      // the sequence number is less than or equal to the previous, the entry must be a duplicate
      // and can safely be ignored.
      if (sequenceNumber == 0 || sequenceNumber > mPreviousSequenceNumber) {
        applyJournalEntry(entry);
        mPreviousSequenceNumber = sequenceNumber;
      }
    }
  }

  @Override
  public void install(SnapshotReader snapshotReader) {
    resetState();
    JournalEntryStreamReader reader =
        new JournalEntryStreamReader(new SnapshotReaderStream(snapshotReader));

    while (snapshotReader.hasRemaining()) {
      JournalEntry entry;
      try {
        entry = reader.readEntry();
      } catch (IOException e) {
        LOG.error("Failed to install snapshot. Terminating process to prevent inconsistency.", e);
        System.exit(-1);
        throw new IllegalStateException(e); // We should never reach here.
      }
      applyJournalEntry(entry);
    }
    LOG.info("Successfully installed snapshot");
  }
}
