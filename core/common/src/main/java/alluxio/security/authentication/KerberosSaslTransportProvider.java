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
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.LoginUser;
import alluxio.security.util.KerberosUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
  public TTransport getClientTransport(InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    Subject subject = LoginUser.getClientLoginSubject();
    String serviceName = KerberosUtils.getKerberosServiceName();
    return getClientTransportInternal(subject, serviceName, serverAddress.getHostName(),
        serverAddress);
  }

  @Override
  public TTransport getClientTransport(Subject subject, InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    if (subject == null) {
      subject = LoginUser.getClientLoginSubject();
    }
    String serviceName = KerberosUtils.getKerberosServiceName();
    return getClientTransportInternal(
        subject, serviceName, serverAddress.getHostName(), serverAddress);
  }

  /**
   * Gets a client thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol Thrift SASL protocol name
   * @param serverName Thrift SASL server name
   * @param serverAddress thrift server address
   * @return Thrift transport
   */
  public TTransport getClientTransportInternal(
      Subject subject, final String protocol, final String serverName,
      final InetSocketAddress serverAddress) throws UnauthenticatedException {
    try {
      return Subject.doAs(subject, new
          PrivilegedExceptionAction<TSaslClientTransport>() {
        public TSaslClientTransport run() throws AuthenticationException {
          try {
            TTransport wrappedTransport =
                TransportProviderUtils.createThriftSocket(serverAddress, mSocketTimeoutMs);
            return new TSaslClientTransport(
                KerberosUtils.GSSAPI_MECHANISM_NAME, null /* authorizationId */,
                protocol, serverName, KerberosUtils.SASL_PROPERTIES, null, wrappedTransport);
          } catch (SaslException e) {
            throw new AuthenticationException("Exception initializing SASL client", e);
          }
        }
      });
    } catch (PrivilegedActionException e) {
      Throwables.propagateIfPossible(e.getCause(), UnauthenticatedException.class);
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public TTransportFactory getServerTransportFactory(String serverName) throws SaslException {
    return getServerTransportFactory(new Runnable() {
      @Override
      public void run() {}
    }, serverName);
  }

  @Override
  public TTransportFactory getServerTransportFactory(Runnable runnable, String serverName)
      throws SaslException {
    try {
      Subject subject = LoginUser.getServerLoginSubject();
      String serviceName = KerberosUtils.getKerberosServiceName();
      Preconditions.checkNotNull(serverName);
      return getServerTransportFactoryInternal(subject, serviceName, serverName,
          runnable);
    } catch (IOException | PrivilegedActionException e) {
      throw new SaslException("Failed to create KerberosSaslServer : ", e);
    }
  }

  /**
   * Gets a server thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol Thrift SASL protocol name
   * @param serverName Thrift SASL server name
   * @param callback the callback runs after the transport is established
   * @return a server transport
   * @throws SaslException when SASL can't be initialized
   * @throws PrivilegedActionException when the Subject doAs failed
   */
  public TTransportFactory getServerTransportFactoryInternal(Subject subject, final String protocol,
      final String serverName, final Runnable callback)
      throws SaslException, PrivilegedActionException {
    return Subject.doAs(subject, new PrivilegedExceptionAction<TSaslServerTransport.Factory>() {
      public TSaslServerTransport.Factory run() {
        TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
        saslTransportFactory
            .addServerDefinition(KerberosUtils.GSSAPI_MECHANISM_NAME, protocol, serverName,
                KerberosUtils.SASL_PROPERTIES,
                new KerberosUtils.ThriftGssSaslCallbackHandler(callback));
        return saslTransportFactory;
      }
    });
  }
}
