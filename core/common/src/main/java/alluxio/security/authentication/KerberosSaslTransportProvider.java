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
import alluxio.Constants;
import alluxio.security.LoginUser;

import com.google.common.base.Preconditions;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

/**
 * Sasl transport provider when authentication type is {@link AuthType#KERBEROS).
 */
@ThreadSafe
public final class KerberosSaslTransportProvider implements TransportProvider {
  /** Timtout for socket in ms. */
  private int mSocketTimeoutMs;
  /** Configuration. */
  private Configuration mConfiguration;

  /** CallbackHandler for SASL GSSAPI Kerberos mechanism. */
  public static class SaslGssCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws
        UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL GSSAPI Callback");
        }
      }

      // TODO(chaomin): ? KerberosAuthenticationProvider.authenticate(subject);
      if (ac != null) {
        String authid = ac.getAuthenticationID();
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          ac.setAuthorizedID(authzid);
          // After verification succeeds, a user with this authz id will be set to a Threadlocal.
          AuthenticatedClientUser.set(authzid);
        }
      }
    }
  }

  /**
   * Constructor for transport provider when authentication type is {@link AuthType#KERBEROS).
   * @param conf Alluxio configuration
   */
  public KerberosSaslTransportProvider(Configuration conf) {
    mConfiguration = Preconditions.checkNotNull(conf);
    mSocketTimeoutMs = conf.getInt(Constants.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS);
  }

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress) throws IOException {
    if (!mConfiguration.containsKey(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL)) {
      throw new SaslException(
          "Failed to get client transport: alluxio.security.kerberos.login.principal must be set.");
    }
    String principal = mConfiguration.get(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL);
    final String[] names = principal.split("[/@]");
    if (names.length < 2) {
      throw new AccessControlException(
          "Kerberos principal name does NOT have the expected hostname part: " + principal);
    }
    Subject subject = LoginUser.get(mConfiguration).getSubject();

    try {
      return getClientTransport(subject, names[0], names[1], serverAddress);
    } catch (PrivilegedActionException e) {
      throw new IOException("PrivilegedActionException" + e);
    }
  }

  /**
   * Method to get a client thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol protocol
   * @param serviceName service name
   * @param serverAddress thrift server address
   * @return Thrift transport
   * @throws SaslException when it failed to create a tTransport
   * @throws PrivilegedActionException when the doAs failed
   */
  public TTransport getClientTransport(Subject subject,
                                       final String protocol,
                                       final String serviceName,
                                       final InetSocketAddress serverAddress)
      throws SaslException, PrivilegedActionException {
    return Subject.doAs(subject, new
          PrivilegedExceptionAction<TSaslClientTransport>() {
        public TSaslClientTransport run() throws AuthenticationException {
          try {
            TTransport wrappedTransport =
                TransportProviderUtils.createThriftSocket(serverAddress, mSocketTimeoutMs);
            Map<String, String> saslProperties = new HashMap<String, String>();
            saslProperties.put(Sasl.QOP, "auth");
            return
                new TSaslClientTransport("GSSAPI", null, protocol, serviceName,
                    saslProperties, null, wrappedTransport);
          } catch (SaslException e) {
            throw new AuthenticationException("Exception initializing SASL client", e);
          }
        }
      });
  }

  @Override
  public TTransportFactory getServerTransportFactory() throws SaslException {
    if (!mConfiguration.containsKey(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL)) {
      throw new SaslException(
          "Failed to get server transport: alluxio.security.kerberos.login.principal must be set.");
    }
    String principal = mConfiguration.get(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL);
    final String[] names = principal.split("[/@]");
    if (names.length < 2) {
      throw new AccessControlException(
          "Kerberos principal name does NOT have the expected hostname part: " + principal);
    }
    try {
      Subject subject = LoginUser.get(mConfiguration).getSubject();
      return getServerTransportFactory(subject, names[0], names[1]);
    } catch (PrivilegedActionException e) {
      throw new SaslException("PrivilegedActionException" + e);
    } catch (IOException e) {
      throw new SaslException("IOException" + e);
    }
  }

  /**
   * Method to get a server thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol protocol name
   * @param serviceName service name
   * @return a server transport
   * @throws SaslException when sasl can't be initialized
   * @throws PrivilegedActionException when privileged action is invalid
   */
  public TTransportFactory getServerTransportFactory(
      Subject subject, final String protocol, final String serviceName)
      throws SaslException, PrivilegedActionException {
    final Map<String, String> saslProperties = new HashMap<String, String>();
    saslProperties.put(Sasl.QOP, "auth");

    return Subject.doAs(subject, new PrivilegedExceptionAction<TSaslServerTransport.Factory>() {
        public TSaslServerTransport.Factory run() {
            TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
            saslTransportFactory.addServerDefinition(
                "GSSAPI", protocol, serviceName, saslProperties, new SaslGssCallbackHandler());
            return saslTransportFactory;
        }
      });
  }
}
