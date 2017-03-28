/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.privilege;

import alluxio.util.CommonUtils;
import alluxio.wire.Privilege;

import java.io.IOException;
import java.util.List;

/**
 * Class for checking that a user has the right privileges.
 */
public class PrivilegeChecker {
  private final PrivilegeService mPrivilegeService;

  /**
   * @param privilegeService the privilege service to back the privilege checker
   */
  public PrivilegeChecker(PrivilegeService privilegeService) {
    mPrivilegeService = privilegeService;
  }

  /**
   * @param user the user to check privileges for
   * @param privilege the privilege to check
   */
  public void check(String user, Privilege privilege) {
    List<String> groups;
    try {
      groups = CommonUtils.getGroups(user);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (String group : groups) {
      if (mPrivilegeService.hasPrivilege(group, privilege)) {
        return;
      }
    }
    throw new RuntimeException(
        String.format("User %s does not have privilege %s", user, privilege));
  }
}
