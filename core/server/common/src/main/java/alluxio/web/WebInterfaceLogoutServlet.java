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

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * Servlet for logout.
 */
@ThreadSafe
public final class WebInterfaceLogoutServlet extends HttpServlet {
  private static final long serialVersionUID = -3108659433169680739L;

  /**
   * Path for the logout servlet.
   */
  public static final String PATH = "/logout";

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    HttpSession session = request.getSession();
    WebInterfaceLoginServlet.AUTHENTICATION_TOKENS.invalidate(session.getId());
    session.invalidate();
    response.sendRedirect(WebInterfaceLoginServlet.PATH);
  }
}
