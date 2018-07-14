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

package org.apache.hadoop.security;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.Subject;

/**
 * An {@link UserGroupInformation} with additional group information from Alluxio.
 */
@NotThreadSafe
public class AlluxioUserGroupInformation extends UserGroupInformation {
  private final String[] mGroups;

  /**
   * Creates a new instance with Alluxio user group information.
   * @param user the user
   * @param groups the groups for the user
   * @param authenticationType the authentication string
   */
  public AlluxioUserGroupInformation(String user, List<String> groups,
      String authenticationType) {
    super(getSubject(user));
    mGroups = groups.toArray(new String[0]);
    setAuthenticationMethod(SaslRpcServer.AuthMethod.valueOf(authenticationType));
  }

  private static Subject getSubject(String userName) {
    Subject subject = new Subject();
    subject.getPrincipals().add(new User(userName));
    return subject;
  }

  @Override
  public synchronized String[] getGroupNames() {
    return mGroups;
  }
}
