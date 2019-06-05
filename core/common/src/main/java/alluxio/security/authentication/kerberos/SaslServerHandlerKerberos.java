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

package alluxio.security.authentication.kerberos;

import alluxio.conf.AlluxioConfiguration;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authentication.SaslServerHandler;
import alluxio.security.util.KerberosUtils;

import com.google.common.base.Throwables;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * {@link SaslServerHandler} implementation for Kerberos scheme.
 */
public class SaslServerHandlerKerberos implements SaslServerHandler {
  /** Underlying {@code SaslServer}. */
  private final SaslServer mSaslServer;

  /** Authenticated user info.  */
  private AuthenticatedUserInfo mUserInfo;

  /**
   * Creates {@link SaslServerHandler} for Kerberos.
   *
   * @param serverName server name
   * @param serverSubject logged in server subject
   * @param conf Alluxio configuration
   * @throws SaslException
   */
  public SaslServerHandlerKerberos(String serverName, Subject serverSubject,
      AlluxioConfiguration conf) throws SaslException {
    String unifiedInstanceName = KerberosUtils.maybeGetKerberosUnifiedInstanceName(conf);
    final String instanceName = unifiedInstanceName != null ? unifiedInstanceName : serverName;
    CallbackHandler cb = new SaslServerCallbackHandlerKerberos(this, conf);
    try {
      SaslServer saslServer =
          Subject.doAs(serverSubject, new PrivilegedExceptionAction<SaslServer>() {
            public SaslServer run() throws SaslException {
              return Sasl.createSaslServer(KerberosUtils.GSSAPI_MECHANISM_NAME,
                  KerberosUtils.getKerberosServiceName(conf), instanceName,
                  KerberosUtils.SASL_PROPERTIES, cb);
            }
          });
      mSaslServer = new PrivilegedSaslServer(saslServer, serverSubject);
    } catch (PrivilegedActionException pe) {
      Throwables.propagateIfPossible(pe.getCause(), SaslException.class);
      throw new RuntimeException(pe.getCause());
    }
  }

  @Override
  public AuthenticatedUserInfo getAuthenticatedUserInfo() {
    return mUserInfo;
  }

  @Override
  public SaslServer getSaslServer() {
    return mSaslServer;
  }

  @Override
  public void setAuthenticatedUserInfo(AuthenticatedUserInfo userinfo) {
    mUserInfo = userinfo;
  }
}
