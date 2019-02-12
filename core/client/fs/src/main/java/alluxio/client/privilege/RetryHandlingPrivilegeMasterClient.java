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

package alluxio.client.privilege;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.client.file.RetryHandlingFileSystemMasterClient;
import alluxio.client.privilege.options.GetGroupPrivilegesOptions;
import alluxio.client.privilege.options.GetGroupToPrivilegesMappingOptions;
import alluxio.client.privilege.options.GetUserPrivilegesOptions;
import alluxio.client.privilege.options.GrantPrivilegesOptions;
import alluxio.client.privilege.options.RevokePrivilegesOptions;
import alluxio.grpc.GetGroupPrivilegesPRequest;
import alluxio.grpc.GetGroupToPrivilegesMappingPRequest;
import alluxio.grpc.GetUserPrivilegesPRequest;
import alluxio.grpc.GrantPrivilegesPRequest;
import alluxio.grpc.PrivilegeList;
import alluxio.grpc.PrivilegeMasterClientServiceGrpc;
import alluxio.grpc.RevokePrivilegesPRequest;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientConfig;
import alluxio.wire.ClosedSourceGrpcUtils;
import alluxio.wire.Privilege;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the privilege master, used by Alluxio clients.
 */
@ThreadSafe
public final class RetryHandlingPrivilegeMasterClient extends AbstractMasterClient
    implements PrivilegeMasterClient {

  private PrivilegeMasterClientServiceGrpc.PrivilegeMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new {@link RetryHandlingFileSystemMasterClient} instance.
   *
   * @param conf master client configuration
   */
  public  RetryHandlingPrivilegeMasterClient(MasterClientConfig conf) {
    super(conf);
  }

  @Override
  public List<Privilege> getGroupPrivileges(final String group,
      final GetGroupPrivilegesOptions options) throws IOException {
    return retryRPC(
        () -> ClosedSourceGrpcUtils
            .fromProto(mClient.getGroupPrivileges(GetGroupPrivilegesPRequest.newBuilder()
                .setGroup(group).setOptions(options.toProto()).build()).getPrivilegesList()),
        "GetGroupPrivileges");
  }

  @Override
  public List<Privilege> getUserPrivileges(final String user,
      final GetUserPrivilegesOptions options) throws IOException {
    return retryRPC(
        () -> ClosedSourceGrpcUtils.fromProto(mClient.getUserPrivileges(GetUserPrivilegesPRequest
            .newBuilder().setUser(user).setOptions(options.toProto()).build()).getPrivilegesList()),
        "GetUserPrivileges");
  }

  @Override
  public Map<String, List<Privilege>> getGroupToPrivilegesMapping(
      final GetGroupToPrivilegesMappingOptions options) throws IOException {
    return retryRPC(() -> {
      Map<String, List<Privilege>> groupInfo = new HashMap<>();
      for (Map.Entry<String, PrivilegeList> entry : mClient.getGroupToPrivilegesMapping(
          GetGroupToPrivilegesMappingPRequest.newBuilder().setOptions(options.toProto()).build())
          .getGroupPrivilegesMap().entrySet()) {
        groupInfo.put(entry.getKey(),
            ClosedSourceGrpcUtils.fromProto(entry.getValue().getPrivilegesList()));
      }
      return groupInfo;
    }, "GetGroupToPrivilegesMapping");
  }

  @Override
  public List<Privilege> grantPrivileges(final String group,
      final List<Privilege> privileges, final GrantPrivilegesOptions options) throws IOException {
    return retryRPC(() -> ClosedSourceGrpcUtils
        .fromProto(mClient.grantPrivileges(GrantPrivilegesPRequest.newBuilder().setGroup(group)
            .addAllPrivileges(ClosedSourceGrpcUtils.toProto(privileges))
            .setOptions(options.toProto()).build()).getPrivilegesList()),
        "GrantPrivileges");
  }

  @Override
  public List<Privilege> revokePrivileges(final String group,
      final List<Privilege> privileges, final RevokePrivilegesOptions options) throws IOException {
    return retryRPC(() -> ClosedSourceGrpcUtils
        .fromProto(mClient.revokePrivileges(RevokePrivilegesPRequest.newBuilder().setGroup(group)
            .addAllPrivileges(ClosedSourceGrpcUtils.toProto(privileges))
            .setOptions(options.toProto()).build()).getPrivilegesList()),
        "RevokePrivileges");
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.PRIVILEGE_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.PRIVILEGE_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.PRIVILEGE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = PrivilegeMasterClientServiceGrpc.newBlockingStub(mChannel);
  }
}
