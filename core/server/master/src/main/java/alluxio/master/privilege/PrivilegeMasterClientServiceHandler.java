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
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.GetGroupPrivilegesTOptions;
import alluxio.thrift.GetGroupPrivilegesTResponse;
import alluxio.thrift.GetGroupToPrivilegesMappingTOptions;
import alluxio.thrift.GetGroupToPrivilegesMappingTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.GetUserPrivilegesTOptions;
import alluxio.thrift.GetUserPrivilegesTResponse;
import alluxio.thrift.GrantPrivilegesTOptions;
import alluxio.thrift.GrantPrivilegesTResponse;
import alluxio.thrift.PrivilegeMasterClientService;
import alluxio.thrift.RevokePrivilegesTOptions;
import alluxio.thrift.RevokePrivilegesTResponse;
import alluxio.thrift.TPrivilege;
import alluxio.util.CommonUtils;
import alluxio.wire.ClosedSourceThriftUtils;
import alluxio.wire.Privilege;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
  PrivilegeMasterClientServiceHandler(PrivilegeMaster privilegeMaster) {
    Preconditions.checkNotNull(privilegeMaster);
    mPrivilegeMaster = privilegeMaster;
    mSupergroup = Configuration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options)
      throws TException {
    return new GetServiceVersionTResponse(Constants.PRIVILEGE_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public GetGroupPrivilegesTResponse getGroupPrivileges(final String group,
      GetGroupPrivilegesTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<GetGroupPrivilegesTResponse>() {
      @Override
      public GetGroupPrivilegesTResponse call() throws AlluxioException, IOException {
        checkPrivilegesEnabled();
        if (!inSupergroup(AuthenticatedClientUser.getClientUser()) && !inGroup(group)) {
          throw new PermissionDeniedException(String.format(
              "Only members of group '%s' and members of the supergroup '%s' can list privileges for "
                  + "group '%s'",
              group, mSupergroup, group));
        }
        Set<Privilege> privileges;
        if (group.equals(mSupergroup)) {
          privileges = new HashSet<>(Arrays.asList(Privilege.values()));
        } else {
          privileges = mPrivilegeMaster.getPrivileges(group);
        }
        return new GetGroupPrivilegesTResponse(ClosedSourceThriftUtils.toThrift(privileges));
      }
    });
  }

  @Override
  public GetUserPrivilegesTResponse getUserPrivileges(final String user,
      GetUserPrivilegesTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<GetUserPrivilegesTResponse>() {
      @Override
      public GetUserPrivilegesTResponse call() throws AlluxioException, IOException {
        checkPrivilegesEnabled();
        if (!inSupergroup(AuthenticatedClientUser.getClientUser()) && !isCurrentUser(user)) {
          throw new PermissionDeniedException(String.format(
              "Only user '%s' and members of the supergroup '%s' can list privileges for user '%s'",
              user, mSupergroup, user));
        }
        Set<Privilege> privileges;
        if (inSupergroup(user)) {
          privileges = new HashSet<>(Arrays.asList(Privilege.values()));
        } else {
          privileges = PrivilegeUtils.getUserPrivileges(mPrivilegeMaster, user);
        }
        return new GetUserPrivilegesTResponse(ClosedSourceThriftUtils.toThrift(privileges));
      }
    });
  }

  @Override
  public GetGroupToPrivilegesMappingTResponse getGroupToPrivilegesMapping(
      GetGroupToPrivilegesMappingTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG,
        new RpcCallableThrowsIOException<GetGroupToPrivilegesMappingTResponse>() {
          @Override
          public GetGroupToPrivilegesMappingTResponse call() throws AlluxioException, IOException {
            checkPrivilegesEnabled();
            if (!inSupergroup(AuthenticatedClientUser.getClientUser())) {
              throw new PermissionDeniedException(String.format(
                  "Only members of the supergroup '%s' can list all privileges", mSupergroup));
            }
            Map<String, Set<Privilege>> privilegeMap =
                mPrivilegeMaster.getGroupToPrivilegesMapping();
            Map<String, List<TPrivilege>> tprivilegeMap = new HashMap<>();
            for (Map.Entry<String, Set<Privilege>> entry : privilegeMap.entrySet()) {
              tprivilegeMap.put(entry.getKey(), ClosedSourceThriftUtils.toThrift(entry.getValue()));
            }
            return new GetGroupToPrivilegesMappingTResponse(tprivilegeMap);
          }
        });
  }

  @Override
  public GrantPrivilegesTResponse grantPrivileges(final String group,
      final List<TPrivilege> privileges, GrantPrivilegesTOptions options) throws TException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<GrantPrivilegesTResponse>() {
      @Override
      public GrantPrivilegesTResponse call() throws AlluxioException, IOException {
        checkPrivilegesEnabled();
        if (inSupergroup(AuthenticatedClientUser.getClientUser())) {
          return new GrantPrivilegesTResponse(ClosedSourceThriftUtils.toThrift(mPrivilegeMaster
              .updatePrivileges(group, ClosedSourceThriftUtils.fromThrift(privileges), true)));
        }
        throw new PermissionDeniedException(
            String.format("Only members of the supergroup '%s' can grant privileges", mSupergroup));
      }
    });
  }

  @Override
  public RevokePrivilegesTResponse revokePrivileges(final String group,
      final List<TPrivilege> privileges, RevokePrivilegesTOptions options) throws TException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<RevokePrivilegesTResponse>() {
      @Override
      public RevokePrivilegesTResponse call() throws AlluxioException, IOException {
        checkPrivilegesEnabled();
        if (inSupergroup(AuthenticatedClientUser.getClientUser())) {
          return new RevokePrivilegesTResponse(ClosedSourceThriftUtils.toThrift(mPrivilegeMaster
              .updatePrivileges(group, ClosedSourceThriftUtils.fromThrift(privileges), false)));
        }
        throw new PermissionDeniedException(String
            .format("Only members of the supergroup '%s' can revoke privileges", mSupergroup));
      }
    });
  }

  /**
   * Checks whether privileges are enabled.
   *
   * @throws FailedPreconditionException if privileges are not enabled
   */
  private void checkPrivilegesEnabled() throws FailedPreconditionException {
    if (!Configuration.getBoolean(PropertyKey.SECURITY_PRIVILEGES_ENABLED)) {
      throw new FailedPreconditionException(
          String.format("Privilege controls are disabled. To enable them, set %s=true in master "
              + "configuration", PropertyKey.SECURITY_PRIVILEGES_ENABLED));
    }
    if (Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        .equals(AuthType.NOSASL)) {
      throw new FailedPreconditionException(String.format(
          "Privilege controls are disabled because authentication is disabled by %s=%s",
          PropertyKey.SECURITY_AUTHENTICATION_TYPE.toString(), AuthType.NOSASL.toString()));
    }
  }

  /**
   * Checks that the given user is in the Alluxio supergroup.
   *
   * @param user the user to check
   * @return whether the current authenticated user is in the supergroup
   */
  private boolean inSupergroup(String user) throws AccessControlException, IOException {
    return CommonUtils.getGroups(user).contains(mSupergroup);
  }

  /**
   * @param user the user to check
   * @return whether the given user is the authenticated client user
   */
  private boolean isCurrentUser(String user) throws AccessControlException {
    return user.equals(AuthenticatedClientUser.getClientUser());
  }

  /**
   * @param group the group to check
   * @return whether the current authenticated client user is in the given group
   */
  private boolean inGroup(String group) throws AccessControlException, IOException {
    return CommonUtils.getGroups(AuthenticatedClientUser.getClientUser()).contains(group);
  }
}
