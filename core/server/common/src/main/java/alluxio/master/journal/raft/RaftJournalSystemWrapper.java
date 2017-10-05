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

import alluxio.master.PrimarySelector;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalEntryStateMachine;
import alluxio.master.journal.JournalSystem;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Wrapper around RaftJournalSystem.
 *
 * We use the Raft journal through reflection because it requires java 8, but we want our core
 * codebase to only require java 7. If java 7 users follow a code path which reaches this class, it
 * is expected that they will encounter errors. Raft journal is only supported for java 8+.
 */
public final class RaftJournalSystemWrapper implements JournalSystem {
  // Canonical classname for the Raft journal system.
  private static final String RAFT_CLASS = "alluxio.raft.RaftJournalSystem";
  private static final String GET_PRIMARY_SELECTOR_METHOD = "getPrimarySelector";
  private static final String CREATE_METHOD = "create";
  private final JournalSystem mRaftJournalSystem;

  /**
   * @param conf Raft journal configuration
   */
  public RaftJournalSystemWrapper(RaftJournalConfiguration conf) {
    Class<?> raftJournalSystemClass;
    try {
      raftJournalSystemClass = Class.forName(RAFT_CLASS);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to find RaftJournalSystem class", e);
    }
    Method createMethod;
    try {
      createMethod = raftJournalSystemClass.getMethod(CREATE_METHOD, RaftJournalConfiguration.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Failed to find create(RaftJournalConfiguration) method for RaftJournalSystem", e);
    }
    try {
      mRaftJournalSystem = (JournalSystem) createMethod.invoke(null, conf);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Failed to create RaftJournalSystem", e);
    }
  }

  @Override
  public Journal createJournal(JournalEntryStateMachine stateMachine) {
    return mRaftJournalSystem.createJournal(stateMachine);
  }

  @Override
  public void start() throws InterruptedException, IOException {
    mRaftJournalSystem.start();
  }

  @Override
  public void stop() throws InterruptedException, IOException {
    mRaftJournalSystem.stop();
  }

  @Override
  public void setMode(Mode mode) {
    mRaftJournalSystem.setMode(mode);
  }

  @Override
  public void format() throws IOException {
    mRaftJournalSystem.format();
  }

  @Override
  public boolean isFormatted() throws IOException {
    return mRaftJournalSystem.isFormatted();
  }

  /**
   * @return a leader selector backed by leadership within the Raft cluster
   */
  public PrimarySelector getPrimarySelector() {
    // If we could cast mRaftJournalSystem as a RaftJournalSystem, this implementation would be
    // return ((RaftJournalSystem) mRaftJournalSystem).getPrimarySelector();
    Class<?> raftJournalSystemClass;
    try {
      raftJournalSystemClass = Class.forName(RAFT_CLASS);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to find RaftJournalSystem class", e);
    }
    Method method;
    try {
      method = raftJournalSystemClass.getMethod(GET_PRIMARY_SELECTOR_METHOD);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("Failed to find getPrimarySelector method", e);
    }
    try {
      return (PrimarySelector) method.invoke(mRaftJournalSystem);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Failed to create leader selector", e);
    }
  }
}
