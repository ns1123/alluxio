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

package alluxio.master.policy;

import alluxio.RpcUtils;
import alluxio.grpc.AddPolicyPRequest;
import alluxio.grpc.AddPolicyPResponse;
import alluxio.grpc.ListPolicyPRequest;
import alluxio.grpc.ListPolicyPResponse;
import alluxio.grpc.PolicyInfo;
import alluxio.grpc.PolicyMasterClientServiceGrpc;
import alluxio.grpc.RemovePolicyPRequest;
import alluxio.grpc.RemovePolicyPResponse;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A gRPC handler for privilege master RPCs invoked by Alluxio clients.
 */
public final class PolicyMasterClientServiceHandler
    extends PolicyMasterClientServiceGrpc.PolicyMasterClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(PolicyMasterClientServiceHandler.class);

  private final PolicyMaster mPolicyMaster;

  /**
   * @param policyMaster the {@link PolicyMaster} used to serve RPC requests
   */
  PolicyMasterClientServiceHandler(PolicyMaster policyMaster) {
    Preconditions.checkNotNull(policyMaster);
    mPolicyMaster = policyMaster;
  }

  @Override
  public void listPolicy(ListPolicyPRequest request,
      StreamObserver<ListPolicyPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<ListPolicyPResponse>) () -> {
      List<PolicyInfo> policies = mPolicyMaster.listPolicy(request.getOptions());
      return ListPolicyPResponse.newBuilder().addAllPolicy(policies).build();
    }, "ListPolicy", "", responseObserver);
  }

  @Override
  public void addPolicy(AddPolicyPRequest request,
      StreamObserver<AddPolicyPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<AddPolicyPResponse>) () -> {
      mPolicyMaster
          .addPolicy(request.getAlluxioPath(), request.getDefinition(), request.getOptions());
      return AddPolicyPResponse.newBuilder().build();
    }, "AddPolicy", "", responseObserver);
  }

  @Override
  public void removePolicy(RemovePolicyPRequest request,
      StreamObserver<RemovePolicyPResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<RemovePolicyPResponse>) () -> {
      mPolicyMaster.removePolicy(request.getPolicyName(), request.getOptions());
      return RemovePolicyPResponse.newBuilder().build();
    }, "RemovePolicy", "", responseObserver);
  }
}
