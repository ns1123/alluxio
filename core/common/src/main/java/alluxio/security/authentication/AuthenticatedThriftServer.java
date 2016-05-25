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
import alluxio.security.LoginUser;

import org.apache.thrift.server.TThreadPoolServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

/**
 * Provides Kerberos-aware thrift server thread pool, based on the type of authentication.
 */
public final class AuthenticatedThriftServer extends TThreadPoolServer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Alluxio configuration including authentication configs. */
  private Configuration mConfiguration;
  /** TThreadPoolServer object. */
  private TThreadPoolServer mServer;
  /** Kerberos subject. */
  private Subject mSubject = null;

  /**
   * Constructor for {@link AuthenticatedThriftServer}, with authentication configurations.
   *
   * @param conf Alluxio configuration
   * @param args TThreadPoolServer.Args
   */
  public AuthenticatedThriftServer(Configuration conf, Args args) {
    super(args);

    mConfiguration = conf;
    AuthType authType = conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    switch (authType) {
      case KERBEROS:
        setKerberosThriftServer(args);
        break;
      case NOSASL: // intended to fall through
      case SIMPLE: // intended to fall through
      case CUSTOM:
        mServer = new TThreadPoolServer(args);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported authentication type: " + authType.getAuthName());
    }
  }

  private void setKerberosThriftServer(final Args args) {
    try {
      mSubject = LoginUser.getServerLoginSubject(mConfiguration);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return;
    }
    if (mSubject == null) {
      LOG.error("In Kerberos mode, failed to get a valid subject.");
      return;
    }
    try {
      TThreadPoolServer server = Subject.doAs(mSubject,
          new PrivilegedExceptionAction<TThreadPoolServer>() {
            public TThreadPoolServer run() throws Exception {
              return new TThreadPoolServer(args);
            }
          });
      mServer = server;
    } catch (PrivilegedActionException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public void serve() {
    AuthType authType = mConfiguration.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.class);
    switch (authType) {
      case KERBEROS:
        kerberosServe();
        break;
      case NOSASL: // intended to fall through
      case SIMPLE: // intended to fall through
      case CUSTOM:
        mServer.serve();
        break;
      default:
        throw new UnsupportedOperationException(
            "createThreadPoolServer: Unsupported authentication type: " + authType.getAuthName());
    }
  }

  private void kerberosServe() {
    if (mSubject == null) {
      LOG.error("In Kerberos mode, failed to get a valid subject.");
      return;
    }
    try {
      Subject.doAs(mSubject,
          new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
              mServer.serve();
              return null;
            }
          });
    } catch (PrivilegedActionException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public void stop() {
    AuthType authType = mConfiguration.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE,
        AuthType.class);
    switch (authType) {
      case KERBEROS:
        kerberosStop();
        break;
      case NOSASL: // intended to fall through
      case SIMPLE: // intended to fall through
      case CUSTOM:
        mServer.stop();
        break;
      default:
        throw new UnsupportedOperationException(
            "createThreadPoolServer: Unsupported authentication type: " + authType.getAuthName());
    }
  }

  private void kerberosStop() {
    if (mSubject == null) {
      LOG.error("In Kerberos mode, failed to get a valid subject.");
      return;
    }
    try {
      Subject.doAs(mSubject,
          new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
              mServer.stop();
              return null;
            }
          });
    } catch (PrivilegedActionException e) {
      LOG.error(e.getMessage(), e);
    }
  }
}

