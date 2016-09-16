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
 * SASL transport provider when authentication type is {@link AuthType#KERBEROS).
 */
@ThreadSafe
public final class KerberosSaslTransportProvider implements TransportProvider {
  private static final String GSSAPI_MECHANISM_NAME = "GSSAPI";
  /** Timeout for socket in ms. */
  private final int mSocketTimeoutMs;

  /** SASL properties. */
  private static final Map<String, String> SASL_PROPERTIES = new HashMap<String, String>() {
    {
      put(Sasl.QOP, "auth");
    }
  };

  /**
   * CallbackHandler for SASL GSSAPI Kerberos mechanism.
   */
  private static final class GssSaslCallbackHandler implements CallbackHandler {
    /**
     * Creates a new instance of {@link GssSaslCallbackHandler}.
     */
    public GssSaslCallbackHandler() {}

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL GSSAPI Callback");
        }
      }

      if (ac != null) {
        // Extract and verify the Kerberos id, which is the full principal name.
        // Currently because Kerberos impersonation is not supported, authenticationId and
        // authorizationId must match in order to make Kerberos login succeed.
        String authenticationId = ac.getAuthenticationID();
        String authorizationId = ac.getAuthorizationID();
        if (authenticationId.equals(authorizationId)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          ac.setAuthorizedID(authorizationId);
          // After verification succeeds, a user with this authorizationId will be set to a
          // Threadlocal.
          AuthenticatedClientUser.set(new KerberosName(authorizationId).getServiceName());
        }
        // Do not set the AuthenticatedClientUser if the user is not authorized.
      }
    }
  }

  /**
   * Constructor for transport provider when authentication type is {@link AuthType#KERBEROS).
   */
  public KerberosSaslTransportProvider() {
    mSocketTimeoutMs = Configuration.getInt(PropertyKey.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS);
  }

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress) throws IOException {
    KerberosName name = getServerKerberosName();
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
                GSSAPI_MECHANISM_NAME, null /* authorizationId */,
                protocol, serviceName, SASL_PROPERTIES, null, wrappedTransport);
          } catch (SaslException e) {
            throw new AuthenticationException("Exception initializing SASL client", e);
          }
        }
      });
  }

  @Override
  public TTransportFactory getServerTransportFactory() throws SaslException {
    KerberosName name = getServerKerberosName();

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
                GSSAPI_MECHANISM_NAME, protocol, serviceName, SASL_PROPERTIES,
                new GssSaslCallbackHandler());
            return saslTransportFactory;
        }
      });
  }

  /**
   * Parses a server Kerberos principal, which is stored in
   * {@link PropertyKey#SECURITY_KERBEROS_SERVER_PRINCIPAL}.
   *
   * @return a list of strings representing three parts: the primary, the instance, and the realm
   * @throws AccessControlException if server principal config is invalid
   * @throws SaslException if server principal config is not specified
   */
  private KerberosName getServerKerberosName() throws AccessControlException, SaslException {
    String principal = Configuration.get(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL);
    if (principal.isEmpty()) {
      throw new SaslException("Failed to parse server principal: "
          + PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL.toString() + " must be set.");
    }
    return new KerberosName(principal);
  }
}
