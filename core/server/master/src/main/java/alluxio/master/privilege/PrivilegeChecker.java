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

package alluxio.master.privilege;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;
import alluxio.wire.Privilege;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for checking that a user has the right privileges.
 */
@ThreadSafe
public class PrivilegeChecker {
  private final PrivilegeMaster mPrivilegeMaster;

  /**
   * @param privilegeMaster the privilege master
   */
  public PrivilegeChecker(PrivilegeMaster privilegeMaster) {
    mPrivilegeMaster = privilegeMaster;
  }

  /**
   * Checks whether the authenticated client user has the given privilege.
   *
   * @param privilege the privilege to check
   */
  public void check(Privilege privilege)
      throws PermissionDeniedException, UnauthenticatedException {
    if (Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        .equals(AuthType.NOSASL)) {
      return;
    }
    try {
      check(AuthenticatedClientUser.getClientUser(), privilege);
    } catch (AccessControlException e) {
      throw new UnauthenticatedException("Failed to get the authenticated client user", e);
    }
  }

  /**
   * @param user the user to check privileges for
   * @param privilege the privilege to check
   */
  public void check(String user, Privilege privilege) throws PermissionDeniedException {
    if (!Configuration.getBoolean(PropertyKey.SECURITY_PRIVILEGES_ENABLED)) {
      return;
    }
    List<String> groups;
    try {
      groups = CommonUtils.getGroups(user);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (String group : groups) {
      if (mPrivilegeMaster.hasPrivilege(group, privilege)) {
        return;
      }
    }
    throw new PermissionDeniedException(
        ExceptionMessage.PRIVILEGE_DENIED.getMessage(user, privilege));
  }
}