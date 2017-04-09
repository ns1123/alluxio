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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.GetGroupPrivilegesTOptions;
import alluxio.thrift.GetGroupToPrivilegesMappingTOptions;
import alluxio.thrift.GetUserPrivilegesTOptions;
import alluxio.thrift.GrantPrivilegesTOptions;
import alluxio.thrift.PrivilegeMasterClientService;
import alluxio.thrift.RevokePrivilegesTOptions;
import alluxio.thrift.TPrivilege;
import alluxio.util.CommonUtils;
import alluxio.wire.ClosedSourceThriftUtils;
import alluxio.wire.Privilege;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Thrift handler for privilege master RPCs invoked by Alluxio clients.
 */
public final class PrivilegeMasterClientServiceHandler
    implements PrivilegeMasterClientService.Iface {
  private static final Logger LOG =
      LoggerFactory.getLogger(PrivilegeMasterClientServiceHandler.class);

  private final PrivilegeMaster mPrivilegeMaster;
  private final String mSupergroup;

  /**
   * @param privilegeMaster the {@link PrivilegeMaster} used to serve RPC requests
   */
  public PrivilegeMasterClientServiceHandler(PrivilegeMaster privilegeMaster) {
    Preconditions.checkNotNull(privilegeMaster);
    mPrivilegeMaster = privilegeMaster;
    mSupergroup = Configuration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
  }

  @Override
  public long getServiceVersion() throws TException {
    return Constants.PRIVILEGE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public List<TPrivilege> getGroupPrivileges(final String group, GetGroupPrivilegesTOptions options)
      throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<List<TPrivilege>>() {
      @Override
      public List<TPrivilege> call() throws AlluxioException {
        checkPrivilegesEnabled();
        if (inSupergroup() || inGroup(group)) {
          return ClosedSourceThriftUtils.toThrift(mPrivilegeMaster.getPrivileges(group));
        }
        throw new RuntimeException(String.format(
            "Only members of group '%s' and members of the supergroup '%s' can list privileges for "
                + "group '%s'", group, mSupergroup, group));
      }
    });
  }

  @Override
  public List<TPrivilege> getUserPrivileges(final String user, GetUserPrivilegesTOptions options)
      throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<List<TPrivilege>>() {
      @Override
      public List<TPrivilege> call() throws AlluxioException {
        checkPrivilegesEnabled();
        if (inSupergroup() || isCurrentUser(user)) {
          return ClosedSourceThriftUtils
              .toThrift(PrivilegeUtils.getUserPrivileges(mPrivilegeMaster, user));
        }
        throw new RuntimeException(String.format(
            "Only user '%s' and members of the supergroup '%s' can list privileges for user '%s'",
                user, mSupergroup, user));
      }
    });
  }

  @Override
  public Map<String, List<TPrivilege>> getGroupToPrivilegesMapping(
      GetGroupToPrivilegesMappingTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<Map<String, List<TPrivilege>>>() {
      @Override
      public Map<String, List<TPrivilege>> call() throws AlluxioException {
        checkPrivilegesEnabled();
        if (!inSupergroup()) {
          throw new RuntimeException(String.format(
              "Only members of the supergroup '%s' can list all privileges", mSupergroup));
        }
        Map<String, Set<Privilege>> privilegeMap = mPrivilegeMaster.getGroupToPrivilegesMapping();
        Map<String, List<TPrivilege>> tprivilegeMap = new HashMap<>();
        for (Map.Entry<String, Set<Privilege>> entry : privilegeMap.entrySet()) {
          tprivilegeMap.put(entry.getKey(), ClosedSourceThriftUtils.toThrift(entry.getValue()));
        }
        return tprivilegeMap;
      }
    });
  }

  @Override
  public List<TPrivilege> grantPrivileges(final String group, final List<TPrivilege> privileges,
      GrantPrivilegesTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<List<TPrivilege>>() {
      @Override
      public List<TPrivilege> call() throws AlluxioException {
        checkPrivilegesEnabled();
        if (inSupergroup()) {
          return ClosedSourceThriftUtils.toThrift(mPrivilegeMaster.updatePrivileges(group,
              ClosedSourceThriftUtils.fromThrift(privileges), true));
        }
        throw new RuntimeException(String.format(
            "Only members of the supergroup '%s' can grant privileges", mSupergroup));
      }
    });
  }

  @Override
  public List<TPrivilege> revokePrivileges(final String group, final List<TPrivilege> privileges,
      RevokePrivilegesTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<List<TPrivilege>>() {
      @Override
      public List<TPrivilege> call() throws AlluxioException {
        checkPrivilegesEnabled();
        if (inSupergroup()) {
          return ClosedSourceThriftUtils.toThrift(mPrivilegeMaster.updatePrivileges(group,
              ClosedSourceThriftUtils.fromThrift(privileges), false));
        }
        throw new RuntimeException(String.format(
            "Only members of the supergroup '%s' can revoke privileges", mSupergroup));
      }
    });
  }

  /**
   * Checks whether privileges are enabled, throwing a runtime exception if they aren't.
   */
  private void checkPrivilegesEnabled() {
    if (!Configuration.getBoolean(PropertyKey.SECURITY_PRIVILEGES_ENABLED)) {
      throw new RuntimeException(
          String.format("Privilege controls are disabled. To enable them, set %s=true in master "
              + "configuration", PropertyKey.SECURITY_PRIVILEGES_ENABLED));
    }
    if (Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        .equals(AuthType.NOSASL)) {
      throw new RuntimeException(String.format(
          "Privilege controls are disabled because authentication is disabled by %s=%s",
          PropertyKey.SECURITY_AUTHENTICATION_TYPE.toString(), AuthType.NOSASL.toString()));
    }
  }

  /**
   * Checks that the current user is in the Alluxio supergroup. If they aren't, a runtime exception
   * will be thrown.
   *
   * @return whether the current authenticated user is in the supergroup
   */
  private boolean inSupergroup() {
    try {
      String user = AuthenticatedClientUser.getClientUser();
      return CommonUtils.getGroups(user).contains(mSupergroup);
    } catch (AccessControlException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param user the user to check
   * @return whether the given user is the authenticated client user
   */
  private boolean isCurrentUser(String user) {
    try {
      return user.equals(AuthenticatedClientUser.getClientUser());
    } catch (AccessControlException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param group the group to check
   * @return whether the current authenticated client user is in the given group
   */
  private boolean inGroup(String group) {
    try {
      return CommonUtils.getGroups(AuthenticatedClientUser.getClientUser()).contains(group);
    } catch (AccessControlException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
