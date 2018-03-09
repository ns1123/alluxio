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

package alluxio.hadoop;

import alluxio.security.authentication.KerberosLoginProvider;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

/**
 * Provides Kerberos authentication using Hadoop authentication APIs.
 */
public class HadoopKerberosLoginProvider implements KerberosLoginProvider {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopKerberosLoginProvider.class);

  @Override
  public Subject login() throws LoginException {
    Subject subject = null;
    try {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      if (ugi.hasKerberosCredentials()) {
        subject = ugi.doAs(
            (PrivilegedExceptionAction<Subject>) () -> {
              AccessControlContext context =
                  AccessController.getContext();
              return Subject.getSubject(context);
            });
      }
    } catch (Exception e) {
      throw new LoginException(String.format("Failed to login with Hadoop user: %s",
          e.getMessage()));
    }
    if (subject == null) {
      throw new LoginException("could not retrieve subject from UserGroupInformation");
    }
    javax.security.auth.kerberos.KerberosTicket tgt =
        alluxio.security.util.KerberosUtils.extractOriginalTGTFromSubject(subject);
    if (tgt == null) {
      throw new LoginException("could not retrieve TGT from subject");
    }
    return subject;
  }

  @Override
  public void relogin() throws LoginException {
    try {
      if (UserGroupInformation.isLoginKeytabBased()) {
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
      }
    } catch (IOException e) {
      throw new LoginException(String.format("Failed to relogin using hadoop user: %s.",
          e.getMessage()));
    }
  }

  @Override
  public boolean hasKerberosCrendentials() {
    try {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      return ugi.hasKerberosCredentials();
    } catch (IOException e) {
      LOG.debug("could not log in using UserGroupInformation {}", e.getMessage());
      return false;
    }
  }
}
