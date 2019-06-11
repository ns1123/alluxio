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

import alluxio.Client;
import alluxio.grpc.AddPolicyPOptions;
import alluxio.grpc.ListPolicyPOptions;
import alluxio.grpc.PolicyInfo;
import alluxio.grpc.RemovePolicyPOptions;
import alluxio.master.MasterClientContext;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A client to use for interacting with a policy master.
 */
@ThreadSafe
public interface PolicyMasterClient extends Client {

  /**
   * Factory for {@link PolicyMasterClient}.
   */
  class Factory {
    private Factory() {
    } // prevent instantiation

    /**
     * Factory method for {@link PolicyMasterClient}.
     *
     * @param conf master client configuration
     * @return a new {@link PolicyMasterClient} instance
     */
    public static PolicyMasterClient create(MasterClientContext conf) {
      return new RetryHandlingPolicyMasterClient(conf);
    }
  }

  /**
   * Adds a policy definition to an Alluxio path.
   *
   * @param alluxioPath the alluxio path
   * @param definition the policy definition
   * @param options the options
   */
  void addPolicy(String alluxioPath, String definition, AddPolicyPOptions options)
      throws IOException;

  /**
   * Returns the policy definitions.
   *
   * @param options the options
   * @return the list of policy definitions
   */
  List<PolicyInfo> listPolicy(ListPolicyPOptions options) throws IOException;

  /**
   * Removes a policy with the given name.
   *
   * @param policyName the name of the policy to remove
   * @param options the options
   */
  void removePolicy(String policyName, RemovePolicyPOptions options) throws IOException;
}
