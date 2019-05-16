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

package alluxio.security.user;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.util.KerberosUtils;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

/**
 * UserState implementation for logging in with an existing Hadoop login.
 */
public class HadoopKerberosUserState extends BaseUserState {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopKerberosUserState.class);

  /**
   * Factory class to create the user state.
   */
  public static class Factory implements UserStateFactory {
    @Override
    public UserState create(Subject subject, AlluxioConfiguration conf, boolean isServer) {
      AuthType authType = conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
      if (!authType.equals(AuthType.KERBEROS)) {
        LOG.debug("N/A: auth type is not KERBEROS. authType: {}", authType.getAuthName());
        return null;
      }
      // TODO(gpang): check UserGroupInformation.isLoginKeytabBased() here?
      KerberosTicket tgt = KerberosUtils.extractOriginalTGTFromSubject(subject);
      if (tgt == null) {
        LOG.debug("N/A: no kerberos tgt available");
        return null;
      }
      return new HadoopKerberosUserState(subject, conf);
    }
  }

  private HadoopKerberosUserState(Subject subject, AlluxioConfiguration conf) {
    super(subject, conf);
  }

  @Override
  public User login() throws UnauthenticatedException {
    try {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      ugi.getShortUserName();
      Set<User> users = mSubject.getPrincipals(User.class);
      User user = null;
      if (users.isEmpty()) {
        // TODO(gpang): look into changing the User() class.
        user = new User(ugi.getShortUserName());
        mSubject.getPrincipals().add(user);
      } else {
        user = users.iterator().next();
      }
      return user;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public synchronized User relogin() throws UnauthenticatedException {
    UserGroupInformation loginUser = null;
    try {
      loginUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new UnauthenticatedException(
          String.format("Failed retrieve UGI loginUser to relogin error: %s.", e.getMessage()), e);
    }

    try {
      if (UserGroupInformation.isLoginKeytabBased()) {
        loginUser.checkTGTAndReloginFromKeytab();
      }
    } catch (IOException e) {
      throw new UnauthenticatedException(String
          .format("Failed to relogin using Hadoop UGI. loginUser: %s error: %s.", loginUser,
              e.getMessage()), e);
    }
    return mUser;
  }
}
