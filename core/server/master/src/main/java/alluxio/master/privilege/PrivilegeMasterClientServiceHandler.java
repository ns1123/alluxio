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
import alluxio.RpcUtils;
import alluxio.exception.AccessControlException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.grpc.GetGroupPrivilegesPRequest;
import alluxio.grpc.GetGroupPrivilegesPResponse;
import alluxio.grpc.GetGroupToPrivilegesMappingPResponse;
import alluxio.grpc.GetUserPrivilegesPRequest;
import alluxio.grpc.GetUserPrivilegesPResponse;
import alluxio.grpc.GrantPrivilegesPRequest;
import alluxio.grpc.GrantPrivilegesPResponse;
import alluxio.grpc.PrivilegeList;
import alluxio.grpc.PrivilegeMasterClientServiceGrpc;
import alluxio.grpc.RevokePrivilegesPRequest;
import alluxio.grpc.RevokePrivilegesPResponse;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;
import alluxio.wire.ClosedSourceGrpcUtils;
import alluxio.wire.Privilege;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A gRPC handler for privilege master RPCs invoked by Alluxio clients.
 */
public final class PrivilegeMasterClientServiceHandler
    extends PrivilegeMasterClientServiceGrpc.PrivilegeMasterClientServiceImplBase {
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
  public void getGroupPrivileges(GetGroupPrivilegesPRequest request,
      StreamObserver<GetGroupPrivilegesPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetGroupPrivilegesPResponse>) () -> {
      String group = request.getGroup();
      checkPrivilegesEnabled();
      if (!inSupergroup(AuthenticatedClientUser.getClientUser()) && !inGroup(group)) {
        throw new PermissionDeniedException(String.format(
            "Only members of group '%s' and members of the supergroup '%s' can list privileges for "
                + "group '%s'",
            group, mSupergroup, group));
      }
      return GetGroupPrivilegesPResponse.newBuilder()
          .addAllPrivileges(ClosedSourceGrpcUtils.toProto(mPrivilegeMaster.getPrivileges(group)))
          .build();
    }, "GetGroupPrivileges", "request=s", responseObserver, request);
  }

  @Override
  public void getUserPrivileges(GetUserPrivilegesPRequest request,
      StreamObserver<GetUserPrivilegesPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetUserPrivilegesPResponse>) () -> {
      String user = request.getUser();
      checkPrivilegesEnabled();
      if (!inSupergroup(AuthenticatedClientUser.getClientUser()) && !isCurrentUser(user)) {
        throw new PermissionDeniedException(String.format(
            "Only user '%s' and members of the supergroup '%s' can list privileges for user '%s'",
            user, mSupergroup, user));
      }
      return GetUserPrivilegesPResponse.newBuilder().addAllPrivileges(
          ClosedSourceGrpcUtils.toProto(PrivilegeUtils.getUserPrivileges(mPrivilegeMaster, user)))
          .build();
    }, "GetUserPrivileges", "request=s", responseObserver, request);
  }

  @Override
  public void getGroupToPrivilegesMapping(alluxio.grpc.GetGroupToPrivilegesMappingPRequest request,
      StreamObserver<GetGroupToPrivilegesMappingPResponse> responseObserver) {
    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<GetGroupToPrivilegesMappingPResponse>) () -> {
          checkPrivilegesEnabled();
          if (!inSupergroup(AuthenticatedClientUser.getClientUser())) {
            throw new PermissionDeniedException(String.format(
                "Only members of the supergroup '%s' can list all privileges", mSupergroup));
          }
          Map<String, Set<Privilege>> privilegeMap = mPrivilegeMaster.getGroupToPrivilegesMapping();
          Map<String, PrivilegeList> pprivilegeMap = new HashMap<>();
          for (Map.Entry<String, Set<Privilege>> entry : privilegeMap.entrySet()) {
            pprivilegeMap.put(entry.getKey(), PrivilegeList.newBuilder()
                .addAllPrivileges(ClosedSourceGrpcUtils.toProto(entry.getValue())).build());
          }
          return GetGroupToPrivilegesMappingPResponse.newBuilder()
              .putAllGroupPrivileges(pprivilegeMap).build();
        }, "GetGroupToPrivilegesMapping", "request=s", responseObserver, request);
  }

  @Override
  public void grantPrivileges(GrantPrivilegesPRequest request,
      StreamObserver<GrantPrivilegesPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GrantPrivilegesPResponse>) () -> {
      checkPrivilegesEnabled();
      if (inSupergroup(AuthenticatedClientUser.getClientUser())) {
        return GrantPrivilegesPResponse.newBuilder()
            .addAllPrivileges(
                ClosedSourceGrpcUtils.toProto(mPrivilegeMaster.updatePrivileges(request.getGroup(),
                    ClosedSourceGrpcUtils.fromProto(request.getPrivilegesList()), true)))
            .build();
      }
      throw new PermissionDeniedException(
          String.format("Only members of the supergroup '%s' can grant privileges", mSupergroup));
    }, "GrantPrivileges", "request=s", responseObserver, request);
  }

  @Override
  public void revokePrivileges(RevokePrivilegesPRequest request,
      StreamObserver<RevokePrivilegesPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<RevokePrivilegesPResponse>) () -> {
      checkPrivilegesEnabled();
      if (inSupergroup(AuthenticatedClientUser.getClientUser())) {
        return RevokePrivilegesPResponse.newBuilder()
            .addAllPrivileges(
                ClosedSourceGrpcUtils.toProto(mPrivilegeMaster.updatePrivileges(request.getGroup(),
                    ClosedSourceGrpcUtils.fromProto(request.getPrivilegesList()), false)))
            .build();
      }
      throw new PermissionDeniedException(
          String.format("Only members of the supergroup '%s' can revoke privileges", mSupergroup));
    }, "RevokePrivileges", "request=s", responseObserver, request);
  }

  /**
   * Checks whether privileges are enabled.
   *
   * @throws FailedPreconditionException if privileges are not enabled
   */
  private void checkPrivilegesEnabled() throws FailedPreconditionException {
    if (!Configuration.getBoolean(PropertyKey.SECURITY_PRIVILEGES_ENABLED)) {
      throw new FailedPreconditionException(String.format(
          "Privilege controls are disabled. To enable them, set %s=true in master "
              + "configuration", PropertyKey.SECURITY_PRIVILEGES_ENABLED));
    }
    if (Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class).equals(
        AuthType.NOSASL)) {
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
