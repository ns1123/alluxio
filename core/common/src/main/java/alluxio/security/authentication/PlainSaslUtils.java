/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authentication;

import alluxio.Configuration;

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.security.Security;
import java.util.HashMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.sasl.SaslException;

/**
 * Because the Java SunSASL provider doesn't support the server-side PLAIN mechanism. There is a new
 * provider {@link PlainSaslProvider} needed to support server-side PLAIN mechanism.
 * PlainSaslHelper is used to register this provider. It also provides methods to generate PLAIN
 * transport for server and client.
 */
@ThreadSafe
public final class PlainSaslUtils {
  static {
    Security.addProvider(new PlainSaslProvider());
  }

  /**
   * For server side, get a PLAIN mechanism {@link TTransportFactory}. A callback handler is hooked
   * for specific authentication methods.
   *
   * @param authType the authentication type
   * @param conf {@link Configuration}
   * @return a corresponding TTransportFactory, which is PLAIN mechanism
   * @throws SaslException if an {@link AuthenticationProvider} is not found
   */
  public static TTransportFactory getPlainServerTransportFactory(AuthType authType,
      Configuration conf) throws SaslException {
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    AuthenticationProvider provider =
        AuthenticationProvider.Factory.create(authType, conf);
    saslFactory.addServerDefinition(PlainSaslProvider.MECHANISM, null, null,
        new HashMap<String, String>(), new PlainSaslServerCallbackHandler(provider));

    return saslFactory;
  }

  /**
   * Gets a PLAIN mechanism transport for client side.
   *
   * @param username User Name of PlainClient
   * @param password Password of PlainClient
   * @param wrappedTransport The original Transport
   * @return Wrapped transport with PLAIN mechanism
   * @throws SaslException if an AuthenticationProvider is not found
   */
  public static TTransport getPlainClientTransport(String username, String password,
      TTransport wrappedTransport) throws SaslException {
    return new TSaslClientTransport(PlainSaslProvider.MECHANISM, null, null, null,
        new HashMap<String, String>(), new PlainSaslClientCallbackHandler(username, password),
        wrappedTransport);
  }

}
