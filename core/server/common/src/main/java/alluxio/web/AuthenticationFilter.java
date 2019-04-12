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

import alluxio.Constants;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * Filter for authentication, all requests from web UI must go through this filter before
 * forwarding to other servlets.
 */
@ThreadSafe
public class AuthenticationFilter implements Filter {
  private static final Pattern[] NO_AUTHENTICATION_PATH_PATTERNS = new Pattern[]{
      Pattern.compile(WebInterfaceLoginServlet.PATH),
      Pattern.compile(WebInterfaceLoginServlet.JSP_PATH),
      Pattern.compile(Constants.REST_API_PREFIX),
      Pattern.compile(".*\\.js"),
      Pattern.compile(".*\\.css"),
      Pattern.compile(".*\\.png"),
      Pattern.compile(".*\\.ico")
  };

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // Nothing to initialize.
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    @SuppressWarnings("unchecked")
    HttpServletRequest httpRequest = (HttpServletRequest) request;

    String path = httpRequest.getRequestURI();
    // For login page and static assets like js, css, and images, do not check login status.
    for (Pattern pattern : NO_AUTHENTICATION_PATH_PATTERNS) {
      Matcher matcher = pattern.matcher(path);
      if (matcher.find()) {
        // Forward the request.
        chain.doFilter(request, response);
        return;
      }
    }

    HttpSession session = httpRequest.getSession();
    Object token = session.getAttribute(
        WebInterfaceLoginServlet.SESSION_ATTRIBUTE_AUTHENTICATION_TOKEN);
    if (token != null) {
      String sessionID = session.getId();
      String cachedToken = WebInterfaceLoginServlet.AUTHENTICATION_TOKENS.getIfPresent(sessionID);
      if (cachedToken != null && cachedToken.equals(token)) {
        // User is authenticated, continue.
        chain.doFilter(request, response);
        return;
      }
    }

    // User has not logged in or the authentication key is incorrect,
    // redirect to login page.
    if (response instanceof HttpServletResponse) {
      HttpServletResponse httpResponse = (HttpServletResponse) response;
      httpResponse.sendRedirect(WebInterfaceLoginServlet.PATH);
    }
    throw new IllegalArgumentException("response is not HttpServletResponse");
  }

  @Override
  public void destroy() {
    // Nothing to destroy.
  }
}
