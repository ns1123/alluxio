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

package alluxio.master.policy.meta;

import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.policy.PolicyKey;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Policy.PolicyDefinitionEntry;
import alluxio.proto.journal.Policy.PolicyRemoveEntry;
import alluxio.resource.LockResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * This manages the policy definitions.
 */
public final class PolicyStore implements Journaled {
  private static final Logger LOG = LoggerFactory.getLogger(PolicyStore.class);

  /** The collection of policy definitions. */
  private final Map<String, PolicyDefinition> mPolicies = new ConcurrentHashMap<>(16, 0.90f, 4);
  /**
   * Used for serializing writes to the {@code mPolicies}. Reads do not need to be protected since
   * policy definitions are immutable, and {@link ConcurrentHashMap} provides a concurrent iterator.
   */
  private final Lock mLock = new ReentrantLock();

  /**
   * Creates a new instance of {@link PolicyStore}.
   */
  public PolicyStore() {
  }

  @Override
  public CheckpointName getCheckpointName() {
    // TODO(gpang): implement
    return null;
  }

  @Override
  public boolean processJournalEntry(JournalEntry entry) {
    if (entry.hasPolicyDefinition()) {
      applyAdd(entry.getPolicyDefinition());
    } else if (entry.hasPolicyRemove()) {
      applyRemove(entry.getPolicyRemove());
    } else {
      return false;
    }
    return true;
  }

  @Override
  public void resetState() {
    mPolicies.clear();
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    Iterator<PolicyDefinition> it = mPolicies.values().iterator();
    return new Iterator<JournalEntry>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public JournalEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return it.next().toJournalEntry();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("PolicyStore#Iterator#remove is not supported.");
      }
    };
  }

  /**
   * @return an iterator over all policy definitions
   */
  public Iterator<PolicyDefinition> getPolicies() {
    return mPolicies.values().iterator();
  }

  /**
   * @param name policy name
   * @return the policy if exists, or null otherwise
   */
  public PolicyDefinition getPolicy(String name) {
    return mPolicies.get(name);
  }

  /**
   * @param key policy key
   * @return the policy if exists, or null otherwise
   */
  public PolicyDefinition getPolicy(PolicyKey key) {
    PolicyDefinition policy = mPolicies.get(key.getName());
    if (policy != null && policy.getCreatedAt() == key.getCreatedAt()) {
      return policy;
    }
    return null;
  }

  /**
   * Adds a policy definition.
   *
   * @param context journal context supplier
   * @param policyDefinition the policy definition to add
   * @return true if the policy was added successfully
   */
  public boolean add(Supplier<JournalContext> context, PolicyDefinition policyDefinition) {
    try (LockResource l = new LockResource(mLock)) {
      if (mPolicies.get(policyDefinition.getName()) != null) {
        // Policy definition name already exists
        return false;
      }
      applyAndJournal(context, policyDefinition.toJournalEntry());
      return true;
    }
  }

  /**
   * Removes a policy definition by name.
   *
   * @param context journal context supplier
   * @param name the name of the policy to remove
   * @return true if the policy was removed successfully
   */
  public boolean remove(Supplier<JournalContext> context, String name) {
    try (LockResource l = new LockResource(mLock)) {
      if (mPolicies.get(name) == null) {
        // Policy definition did not exist
        return false;
      }
      applyAndJournal(context, JournalEntry.newBuilder()
          .setPolicyRemove(PolicyRemoveEntry.newBuilder().setName(name).build()).build());
      return true;
    }
  }

  private void applyAdd(PolicyDefinitionEntry entry) {
    mPolicies.put(entry.getName(), PolicyDefinition.fromJournalEntry(entry));
  }

  private void applyRemove(PolicyRemoveEntry entry) {
    mPolicies.remove(entry.getName());
  }
}
