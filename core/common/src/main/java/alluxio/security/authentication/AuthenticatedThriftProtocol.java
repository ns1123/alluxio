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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.security.LoginUser;

import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

/**
 * Provides Kerberos-aware thrift protocol, based on the type of authentication.
 */
public final class AuthenticatedThriftProtocol extends TMultiplexedProtocol {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** TMultiplexedProtocol object. */
  private TMultiplexedProtocol mProtocol;
  /** Kerberos subject. */
  private Subject mSubject = null;

  /**
   * Constructor for {@link AuthenticatedThriftProtocol}, with authentication configurations.
   *
   * @param protocol TProtocol for TMultiplexedProtocol
   * @param serviceName service name for TMultiplexedProtocol
   */
  public AuthenticatedThriftProtocol(final TProtocol protocol, final String serviceName) {
    super(protocol, serviceName);
    AuthType authType = Configuration.getEnum(
        PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    switch (authType) {
      case KERBEROS:
        setKerberosProtocol(protocol, serviceName);
        break;
      case NOSASL: // intended to fall through
      case SIMPLE: // intended to fall through
      case CUSTOM:
        mProtocol = new TMultiplexedProtocol(protocol, serviceName);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported authentication type: " + authType.getAuthName());
    }
  }

  private void setKerberosProtocol(final TProtocol protocol, final String serviceName) {
    try {
      mSubject = LoginUser.getClientLoginSubject();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return;
    }
    if (mSubject == null) {
      LOG.error("In Kerberos mode, failed to get a valid subject.");
      return;
    }
    try {
      mProtocol = Subject.doAs(mSubject,
          new PrivilegedExceptionAction<TMultiplexedProtocol>() {
            public TMultiplexedProtocol run() throws Exception {
              return new TMultiplexedProtocol(protocol, serviceName);
            }
          });
    } catch (PrivilegedActionException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  /**
   * Opens the transport. If the authentication type is {@link AuthType#KERBEROS}, opens the
   * transport as the subject.
   *
   * @throws TTransportException if failed to open the Thrift transport
   */
  public void openTransport() throws TTransportException {
    AuthType authType = Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE,
        AuthType.class);
    final TTransport transport = mProtocol.getTransport();
    switch (authType) {
      case KERBEROS:
        openKerberosTransport(transport);
        break;
      case NOSASL: // intended to fall through
      case SIMPLE: // intended to fall through
      case CUSTOM:
        transport.open();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported authentication type: " + authType.getAuthName());
    }
  }

  private void openKerberosTransport(final TTransport transport) throws TTransportException {
    if (mSubject == null) {
      LOG.error("In Kerberos mode, failed to get a valid subject.");
      return;
    }
    try {
      Subject.doAs(mSubject,
          new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
              transport.open();
              return null;
            }
          });
    } catch (PrivilegedActionException e) {
      throw new TTransportException("Failed to open Kerberos transport" , e);
    }
  }

  /**
   * Closes the transport. If the authentication type is {@link AuthType#KERBEROS}, closes the
   * transport as the subject.
   */
  public void closeTransport() {
    AuthType authType = Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE,
        AuthType.class);
    final TTransport transport = mProtocol.getTransport();
    switch (authType) {
      case KERBEROS:
        closeKerberosTransport(transport);
        break;
      case NOSASL: // intended to fall through
      case SIMPLE: // intended to fall through
      case CUSTOM:
        transport.close();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported authentication type: " + authType.getAuthName());
    }
  }

  private void closeKerberosTransport(final TTransport transport) {
    if (mSubject == null) {
      LOG.error("In Kerberos mode, failed to get a valid subject.");
      return;
    }
    try {
      Subject.doAs(mSubject,
          new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
              transport.close();
              return null;
            }
          });
    } catch (PrivilegedActionException e) {
      LOG.error(e.getMessage(), e);
    }
  }
}

