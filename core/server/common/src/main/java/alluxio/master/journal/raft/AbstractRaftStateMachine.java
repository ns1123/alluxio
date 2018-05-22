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

package alluxio.master.journal.raft;

import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for journal entry based state machines.
 */
public abstract class AbstractRaftStateMachine extends StateMachine implements Snapshottable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRaftStateMachine.class);

  private volatile long mLastAppliedCommitIndex = -1;

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
   * This method is automatically discovered by the Copycat framework.
   *
   * @param commit the commit
   */
  public void applyJournalEntryCommand(Commit<JournalEntryCommand> commit) {
    JournalEntry entry;
    try {
      entry = JournalEntry.parseFrom(commit.command().getSerializedJournalEntry());
    } catch (Exception e) {
      LOG.error("Fatal error: Encountered invalid journal entry in commit: {}.", commit, e);
      System.exit(-1);
      throw new IllegalStateException(e); // We should never reach here.
    }
    try {
      applyEntryInternal(entry);
    } finally {
      Preconditions.checkState(commit.index() > mLastAppliedCommitIndex);
      mLastAppliedCommitIndex = commit.index();
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
    } else if (entry.toBuilder().clearSequenceNumber().build()
        .equals(JournalEntry.getDefaultInstance())) {
      // Ignore empty entries, they are created during snapshotting.
    } else {
      applyJournalEntry(entry);
    }
  }
}
