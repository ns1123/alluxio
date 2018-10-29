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

import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.security.User;
import alluxio.util.SecurityUtils;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An instance of this class represents a client user connecting to {@link PlainSaslServer}.
 *
 * It is maintained in a {@link ThreadLocal} variable based on the Thrift RPC mechanism.
 * {@link org.apache.thrift.server.TThreadPoolServer} allocates a thread to serve a connection
 * from client side and take back it when connection is closed. During the thread alive cycle,
 * all the RPC happens in this thread. These RPC methods implemented in server side could
 * get the client user by this class.
 */
@ThreadSafe
public final class AuthenticatedClientUser {

  /**
   * A {@link ThreadLocal} variable to maintain the client user along with a specific thread.
   */
  private static ThreadLocal<User> sUserThreadLocal = new ThreadLocal<>();

  // ALLUXIO CS ADD
  /**
   * A {@link ThreadLocal} variable to maintain the connection user along with a specific thread.
   */
  private static ThreadLocal<User> sConnectionUserThreadLocal = new ThreadLocal<>();

  /**
   * A {@link ThreadLocal} variable to maintain the authentication method along with a specific thread.
   */
  private static ThreadLocal<String> sConnectionAuthMethod = new ThreadLocal<>();

  // ALLUXIO CS END
  /**
   * Creates a {@link User} and sets it to the {@link ThreadLocal} variable.
   *
   * @param userName the name of the client user
   */
  public static void set(String userName) {
    sUserThreadLocal.set(new User(userName));
  }

  /**
   * Sets {@link User} to the {@link ThreadLocal} variable.
   *
   * @param user the client user object
   */
  public static void set(User user) {
    sUserThreadLocal.set(user);
  }

  // ALLUXIO CS ADD
  /**
   * Creates a connection {@link User} and sets it to the {@link ThreadLocal} variable.
   *
   * @param userName the name of the connection user
   */
  public static void setConnectionUser(String userName) {
    sConnectionUserThreadLocal.set(new User(userName));
  }

  /**
   * Sets the connection authentication method to the {@link ThreadLocal} variable.
   *
   * @param authMethod the name of the authentication method
   */
  public static void setAuthMethod(String authMethod) {
    sConnectionAuthMethod.set(authMethod);
  }

  // ALLUXIO CS END
  /**
   * Gets the {@link User} from the {@link ThreadLocal} variable.
   *
   * @return the client user, null if the user is not present
   */
  // TODO(peis): Fail early if the user is not able to be set to avoid returning null.
  public static User get() throws IOException {
    if (!SecurityUtils.isAuthenticationEnabled()) {
      throw new IOException(ExceptionMessage.AUTHENTICATION_IS_NOT_ENABLED.getMessage());
    }
    return sUserThreadLocal.get();
  }

  /**
   * @return the user or null if the user is not set
   */
  public static User getOrNull() {
    return sUserThreadLocal.get();
  }

  /**
   * Gets the user name from the {@link ThreadLocal} variable.
   *
   * @return the client user in string
   * @throws AccessControlException there is no authenticated user for this thread or
   *         the authentication is not enabled
   */
  public static String getClientUser() throws AccessControlException {
    try {
      User user = get();
      if (user == null) {
        throw new AccessControlException(
            ExceptionMessage.AUTHORIZED_CLIENT_USER_IS_NULL.getMessage());
      }
      return user.getName();
    } catch (IOException e) {
      throw new AccessControlException(ExceptionMessage.AUTHENTICATION_IS_NOT_ENABLED.getMessage());
    }
  }

  // ALLUXIO CS ADD
  /**
   * Gets the connection user name from the {@link ThreadLocal} variable.
   *
   * @return the client user in string, null if the user is not present
   * @throws AccessControlException if the authentication is not enabled
   */
  public static String getConnectionUser() throws AccessControlException {
    if (!SecurityUtils.isAuthenticationEnabled()) {
      throw new AccessControlException(ExceptionMessage.AUTHENTICATION_IS_NOT_ENABLED.getMessage());
    }
    User user = sConnectionUserThreadLocal.get();
    if (user == null) {
      return null;
    }
    return user.getName();
  }

  /**
   * Gets the connection authentication method from the {@link ThreadLocal} variable.
   *
   * @return the client user in string, null if the user is not present
   * @throws AccessControlException if the authentication is not enabled
   */
  public static String getAuthMethod() throws AccessControlException {
    if (!SecurityUtils.isAuthenticationEnabled()) {
      throw new AccessControlException(ExceptionMessage.AUTHENTICATION_IS_NOT_ENABLED.getMessage());
    }
    return sConnectionAuthMethod.get();
  }

  // ALLUXIO CS END
  /**
   * Removes the {@link User} from the {@link ThreadLocal} variable.
   */
  public static synchronized void remove() {
    sUserThreadLocal.remove();
  }

  private AuthenticatedClientUser() {} // prevent instantiation
}
