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

package alluxio.web;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility functions for {@link WebServer}.
 *
 * NOTE: These functions are only used for Alluxio closed source and is declared in .alluxio_cs,
 * putting them in this standalone file is to minimize sync conflict between closed source and
 * open source.
 */
public final class WebServerUtils {
  /**
   * @return whether login and authentication is enabled
   */
  public static boolean isLoginEnabled() {
    return Configuration.getBoolean(PropertyKey.WEB_LOGIN_ENABLED);
  }

  /**
   * Creates a {@link WebInterfaceLoginServlet} and add it to the application context.
   *
   * @param context the application context to add the login servlet to
   */
  public static void addLoginServlet(WebAppContext context) {
    // Generate a mapping from username to password.
    String username = Configuration.get(PropertyKey.WEB_LOGIN_USERNAME);
    String password = Configuration.get(PropertyKey.WEB_LOGIN_PASSWORD);
    Map<String, String> userPasswords = new HashMap<>();
    userPasswords.put(username, password);

    // Add login servlet.
    WebInterfaceLoginServlet loginServlet = new WebInterfaceLoginServlet(userPasswords);
    context.addServlet(new ServletHolder(loginServlet), WebInterfaceLoginServlet.PATH);
  }

  /**
   * Creates a {@link WebInterfaceLogoutServlet} and add it to the application context.
   *
   * @param context the application context to add the logout servlet to
   */
  public static void addLogoutServlet(WebAppContext context) {
    WebInterfaceLogoutServlet logoutServlet = new WebInterfaceLogoutServlet();
    context.addServlet(new ServletHolder(logoutServlet), WebInterfaceLogoutServlet.PATH);
  }

  /**
   * Creates a {@link AuthenticationFilter} and add it to the application context.
   *
   * @param context the application context to add the login servlet to
   */
  public static void addAuthenticationFilter(WebAppContext context) {
    // Add filter for authenticating users.
    AuthenticationFilter filter = new AuthenticationFilter();
    context.addFilter(new FilterHolder(filter), "/*",
        EnumSet.of(javax.servlet.DispatcherType.REQUEST,
            javax.servlet.DispatcherType.FORWARD, javax.servlet.DispatcherType.INCLUDE));
  }
}
