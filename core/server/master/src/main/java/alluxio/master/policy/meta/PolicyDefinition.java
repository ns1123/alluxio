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

import alluxio.grpc.PolicyInfo;
import alluxio.master.policy.action.ActionDefinition;
import alluxio.master.policy.cond.Condition;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Policy.PolicyDefinitionEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the definition of a policy.
 */
public final class PolicyDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(PolicyDefinition.class);

  private final long mId;
  private final String mName;
  private final String mPath;
  private final PolicyScope mScope;
  private final Condition mCondition;
  private final ActionDefinition mAction;
  private final long mCreatedAt;

  /**
   * Creates a new instance of {@link PolicyDefinition}.
   *
   * @param id the id for the policy
   * @param name the policy name
   * @param path the path the policy is defined for
   * @param scope the enforcement scope of the policy
   * @param condition the condition of the policy
   * @param action the policy action
   * @param createdAt the creation timestamp
   */
  public PolicyDefinition(long id, String name, String path, PolicyScope scope, Condition condition,
      ActionDefinition action, long createdAt) {
    mId = id;
    mName = name;
    mPath = path;
    mScope = scope;
    mCondition = condition;
    mAction = action;
    mCreatedAt = createdAt;
  }

  /**
   * @return the policy id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the policy name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the path that the policy is defined for
   */
  public String getPath() {
    return mPath;
  }

  /**
   * @return the enforcement scope of the policy
   */
  public PolicyScope getScope() {
    return mScope;
  }

  /**
   * @return the policy condition
   */
  public Condition getCondition() {
    return mCondition;
  }

  /**
   * @return the action of the policy
   */
  public ActionDefinition getAction() {
    return mAction;
  }

  /**
   * @return the creation timestamp
   */
  public long getCreatedAt() {
    return mCreatedAt;
  }

  /**
   * @return the policy info proto representation
   */
  public PolicyInfo toPolicyInfo() {
    return PolicyInfo.newBuilder()
        .setId(mId)
        .setName(mName)
        .setPath(mPath)
        .setScope(mScope.toString())
        .setCondition(mCondition.serialize())
        .setAction(mAction.serialize())
        .setCreatedAt(mCreatedAt)
        .build();
  }

  /**
   * @return the journal proto representation
   */
  public JournalEntry toJournalEntry() {
    return JournalEntry.newBuilder()
        .setPolicyDefinition(PolicyDefinitionEntry.newBuilder()
            .setId(mId)
            .setName(mName)
            .setPath(mPath)
            .setScope(mScope.toProto())
            .setCondition(mCondition.serialize())
            .setAction(mAction.serialize())
            .setCreatedAt(mCreatedAt)
            .build()).build();
  }

  /**
   * @param entry the proto representation
   * @return the definition from the journal proto representation
   */
  public static PolicyDefinition fromJournalEntry(PolicyDefinitionEntry entry) {
    return new PolicyDefinition(entry.getId(), entry.getName(), entry.getPath(),
        PolicyScope.fromProto(entry.getScope()), Condition.deserialize(entry.getCondition()),
        ActionDefinition.deserialize(entry.getAction()), entry.getCreatedAt());
  }
}
