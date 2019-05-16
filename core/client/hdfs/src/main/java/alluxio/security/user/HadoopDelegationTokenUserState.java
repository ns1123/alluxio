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
import alluxio.security.Credentials;
import alluxio.security.CurrentUser;
import alluxio.security.TokenUtils;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

import javax.security.auth.Subject;

/**
 * UserState implementation for logging in with a delegation token from Hadoop.
 */
public class HadoopDelegationTokenUserState extends BaseUserState {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopDelegationTokenUserState.class);

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
      if (subject.getPrivateCredentials(Credentials.class).isEmpty()) {
        LOG.debug("N/A: Subject does not contain any Credentials");
        return null;
      }
      // alluxio token credentials exist. verify that the hadoop user exists in the subject
      if (subject.getPrincipals(CurrentUser.class).isEmpty()) {
        LOG.debug("N/A: Subject does not contain CurrentUser principal");
        return null;
      }

      // Construct the service name
      String serviceName =
          subject.getPrincipals(CurrentUser.class).iterator().next().getServiceName();
      try {
        serviceName = TokenUtils.buildTokenService(new URI(serviceName), conf);
      } catch (URISyntaxException e) {
        // This should never happen, since the service name is added via URI.toString
        LOG.warn("serviceName does not parse as URI: {}.", serviceName);
      }
      if (serviceName != null) {
        boolean found = false;
        for (Credentials c : subject.getPrivateCredentials(Credentials.class)) {
          if (c.getToken(serviceName) != null) {
            found = true;
            break;
          }
        }
        if (!found) {
          LOG.debug("N/A: Subject does not contain credentials for serviceName: {}", serviceName);
          return null;
        }
      }
      return new HadoopDelegationTokenUserState(subject, conf);
    }
  }

  private HadoopDelegationTokenUserState(Subject subject, AlluxioConfiguration conf) {
    super(subject, conf);
  }

  @Override
  public User login() throws UnauthenticatedException {
    // with delegation token, user is already logged in. only need to retrieve the user
    Set<User> users = mSubject.getPrincipals(User.class);
    if (!users.isEmpty()) {
      return users.iterator().next();
    }

    Set<CurrentUser> set = mSubject.getPrincipals(CurrentUser.class);
    if (set.isEmpty()) {
      throw new UnauthenticatedException("Missing HadoopUser() from subject for Alluxio token.");
    }
    User user = new User(set.iterator().next().getName());
    mSubject.getPrincipals().add(user);
    return user;
  }
}
