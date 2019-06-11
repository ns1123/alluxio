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

package alluxio.client.policy;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.grpc.AddPolicyPOptions;
import alluxio.grpc.AddPolicyPRequest;
import alluxio.grpc.ListPolicyPOptions;
import alluxio.grpc.ListPolicyPRequest;
import alluxio.grpc.PolicyInfo;
import alluxio.grpc.PolicyMasterClientServiceGrpc;
import alluxio.grpc.RemovePolicyPOptions;
import alluxio.grpc.RemovePolicyPRequest;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the policy master, used by Alluxio clients.
 */
@ThreadSafe
public final class RetryHandlingPolicyMasterClient extends AbstractMasterClient
    implements PolicyMasterClient {

  private PolicyMasterClientServiceGrpc.PolicyMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new instance.
   *
   * @param conf master client configuration
   */
  public RetryHandlingPolicyMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.POLICY_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.POLICY_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.POLICY_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = PolicyMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public void addPolicy(String alluxioPath, String definition, AddPolicyPOptions options)
      throws IOException {
    retryRPC(() -> mClient.addPolicy(
        AddPolicyPRequest.newBuilder().setAlluxioPath(alluxioPath).setDefinition(definition)
            .setOptions(options).build()), "AddPolicy");
  }

  @Override
  public List<PolicyInfo> listPolicy(ListPolicyPOptions options) throws IOException {
    return retryRPC(
        () -> mClient.listPolicy(ListPolicyPRequest.newBuilder().setOptions(options).build()),
        "ListPolicy").getPolicyList();
  }

  @Override
  public void removePolicy(String policyName, RemovePolicyPOptions options) throws IOException {
    retryRPC(() -> mClient.removePolicy(
        RemovePolicyPRequest.newBuilder().setPolicyName(policyName).setOptions(options).build()),
        "RemovePolicy");
  }
}
