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

package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.security.LoginUser;
import alluxio.security.util.KerberosName;
import alluxio.security.util.KerberosUtils;

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;

/**
 * SASL transport provider when authentication type is {@link AuthType#KERBEROS).
 */
@ThreadSafe
public final class KerberosSaslTransportProvider implements TransportProvider {
  /** Timeout for socket in ms. */
  private final int mSocketTimeoutMs;

  /**
   * Constructor for transport provider when authentication type is {@link AuthType#KERBEROS).
   */
  public KerberosSaslTransportProvider() {
    mSocketTimeoutMs = Configuration.getInt(PropertyKey.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS);
  }

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress) throws IOException {
    KerberosName name = KerberosUtils.getServerKerberosName();
    Subject subject = LoginUser.getClientLoginSubject();

    try {
      return getClientTransportInternal(
          subject, name.getServiceName(), name.getHostName(), serverAddress);
    } catch (PrivilegedActionException e) {
      throw new IOException("PrivilegedActionException" + e);
    }
  }

  /**
   * Gets a client thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol Thrift SASL protocol name
   * @param serviceName Thrift SASL service name
   * @param serverAddress thrift server address
   * @return Thrift transport
   * @throws SaslException when it failed to create a Thrift transport
   * @throws PrivilegedActionException when the Subject doAs failed
   */
  public TTransport getClientTransportInternal(
      Subject subject, final String protocol, final String serviceName,
      final InetSocketAddress serverAddress) throws SaslException, PrivilegedActionException {
    return Subject.doAs(subject, new
          PrivilegedExceptionAction<TSaslClientTransport>() {
        public TSaslClientTransport run() throws AuthenticationException {
          try {
            TTransport wrappedTransport =
                TransportProviderUtils.createThriftSocket(serverAddress, mSocketTimeoutMs);
            return new TSaslClientTransport(
                KerberosUtils.GSSAPI_MECHANISM_NAME, null /* authorizationId */,
                protocol, serviceName, KerberosUtils.SASL_PROPERTIES, null, wrappedTransport);
          } catch (SaslException e) {
            throw new AuthenticationException("Exception initializing SASL client", e);
          }
        }
      });
  }

  @Override
  public TTransportFactory getServerTransportFactory() throws SaslException {
    KerberosName name = KerberosUtils.getServerKerberosName();

    try {
      Subject subject = LoginUser.getServerLoginSubject();
      return getServerTransportFactoryInternal(subject, name.getServiceName(), name.getHostName());
    } catch (PrivilegedActionException e) {
      throw new SaslException("PrivilegedActionException" + e);
    } catch (IOException e) {
      throw new SaslException("IOException" + e);
    }
  }

  /**
   * Gets a server thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol Thrift SASL protocol name
   * @param serviceName Thrift SASL service name
   * @return a server transport
   * @throws SaslException when SASL can't be initialized
   * @throws PrivilegedActionException when the Subject doAs failed
   */
  public TTransportFactory getServerTransportFactoryInternal(
      Subject subject, final String protocol, final String serviceName)
      throws SaslException, PrivilegedActionException {
    return Subject.doAs(subject, new PrivilegedExceptionAction<TSaslServerTransport.Factory>() {
        public TSaslServerTransport.Factory run() {
            TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
            saslTransportFactory.addServerDefinition(
                KerberosUtils.GSSAPI_MECHANISM_NAME, protocol, serviceName,
                KerberosUtils.SASL_PROPERTIES, new KerberosUtils.ThriftGssSaslCallbackHandler());
            return saslTransportFactory;
        }
      });
  }
}
