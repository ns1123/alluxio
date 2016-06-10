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

package alluxio.underfs.hdfs;

import alluxio.util.SecurityUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * A utility class that lets program code run in a security context provided by the Hadoop security
 * user groups.
 *
 * The secure context will for example pick up authentication information from Kerberos.
 */
public final class HdfsSecurityUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);

  private static Configuration sHdfsConf = new Configuration();

  private static boolean isHdfsSecurityEnabled() {
    UserGroupInformation.setConfiguration(sHdfsConf);
    return UserGroupInformation.isSecurityEnabled();
  }

  /**
   * Runs a method in a security context as login user.
   *
   * @param runner the method to be run
   * @param <T> the return type
   * @return the result of the secure method
   * @throws IOException if something went wrong
   */
  public static <T> T runAsLoginUser(final AlluxioSecuredRunner<T> runner) throws IOException {

    if (!isHdfsSecurityEnabled()) {
      return runner.run();
    }

    UserGroupInformation.setConfiguration(sHdfsConf);

    LOG.debug("login user {}", UserGroupInformation.getLoginUser());
    LOG.debug("current user {}", UserGroupInformation.getCurrentUser());

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    if (!ugi.hasKerberosCredentials()) {
      LOG.error("Kerberos security is enabled but ugi has no Kerberos credentials. "
          + "Please check principal and keytab configurations.");
    }
    try {
      return ugi.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws IOException {
          return runner.run();
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Runs a method in a security context as current user.
   *
   * @param runner the method to be run
   * @param <T> the return type
   * @return the result of the secure method
   * @throws IOException if something went wrong
   */
  public static <T> T runAsCurrentUser(final AlluxioSecuredRunner<T> runner) throws IOException {
    if (!isHdfsSecurityEnabled()) {
      return runner.run();
    }

    UserGroupInformation.setConfiguration(sHdfsConf);

    LOG.debug("UGI login user {}", UserGroupInformation.getLoginUser());
    LOG.debug("UGI current user {}", UserGroupInformation.getCurrentUser());

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    if (!ugi.hasKerberosCredentials()) {
      LOG.error("Kerberos security is enabled but ugi has no Kerberos credentials. "
          + "Please check principal and keytab configurations.");
    }
    try {
      return ugi.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws IOException {
          return runner.run();
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Runs a method in a security context as custom user.
   *
   * @param runner the method to be run
   * @param <T> the return type
   * @param ugi the custome user
   * @return the result of the secure method
   * @throws IOException if something went wrong
   */
  public static <T> T runAs(UserGroupInformation ugi, final AlluxioSecuredRunner<T> runner)
      throws IOException {

    if (!isHdfsSecurityEnabled()) {
      LOG.info("security is not enabled");
      return runner.run();
    }

    ugi.setConfiguration(sHdfsConf);

    LOG.debug("UGI: {}", ugi.toString());
    LOG.debug("UGI login user {}", ugi.getLoginUser());
    LOG.debug("UGI current user {}", ugi.getCurrentUser());

    if (!ugi.hasKerberosCredentials()) {
      LOG.error("Kerberos security is enabled but ugi has no Kerberos credentials. "
          + "Please check principal and keytab configurations.");
    }
    try {
      return ugi.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws IOException {
          return runner.run();
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Interface that holds a method run.
   *
   * @param <T> the return type of run method
   */
  public interface AlluxioSecuredRunner<T> {
    /**
     * method to run.
     * @return anything
     * @throws IOException if something went wrong
     */
    T run() throws IOException;
  }

  /**
   * Private constructor to prevent instantiation.
   */
  private HdfsSecurityUtils() {
    throw new RuntimeException();
  }
}

