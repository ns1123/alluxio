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

package alluxio.security.capability;

import alluxio.proto.security.CapabilityProto;
import alluxio.security.authentication.Token;
import alluxio.security.authentication.TokenIdentifier;

/**
 * Helper for managing capability {@link Token}s.
 */
public class CapabilityToken {

  /**
   * Creates a {@link Token} from a capability.
   *
   * @param protoCapability proto capability
   * @return the token
   */
  public static Token<CapabilityTokenIdentifier> create(
      CapabilityProto.Capability protoCapability) {
    return new Token<>(new CapabilityTokenIdentifier(protoCapability),
        alluxio.util.proto.ProtoUtils.getAuthenticator(protoCapability));
  }

  /**
   * {@link TokenIdentifier} implementation for capability.
   */
  public static class CapabilityTokenIdentifier implements TokenIdentifier {
    private final CapabilityProto.Capability mCapability;

    /**
     * Creates identifier for capability token.
     *
     * @param capability proto capability
     */
    public CapabilityTokenIdentifier(CapabilityProto.Capability capability) {
      mCapability = capability;
    }

    @Override
    public byte[] getBytes() {
      return alluxio.util.proto.ProtoUtils.getContent(mCapability);
    }
  }
}
