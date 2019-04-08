/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.rest;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.SecurityService;
import org.apache.zeppelin.ticket.TicketContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created for org.apache.zeppelin.rest.message.
 */
@RestController
@RequestMapping("/api/login")
public class LoginRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(LoginRestApi.class);
  private static final Gson gson = new Gson();
  private final SecurityService securityService;

  @Autowired
  public LoginRestApi(@Qualifier("NoSecurityService") final SecurityService securityService) {
    this.securityService = securityService;
  }

  @GetMapping(produces = "application/json")
  public ResponseEntity getLogin(final HttpServletRequest request) {
    JsonResponse response = null;
    final Map<String, Cookie> cookieMap = Maps.uniqueIndex(Arrays.asList(request.getCookies()), Cookie::getName);

      try {
          final Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
          response = proceedToLogin(currentUser, null);


        if (null == response) {
          LOG.warn("No Kerberos token received");
          response = new JsonResponse(HttpStatus.UNAUTHORIZED, "", null);
        }
        return response.build();
      } catch (final AuthenticationException e) {
        LOG.error("Error in Login: " + e);
      }


    return new JsonResponse(HttpStatus.METHOD_NOT_ALLOWED).build();
  }

  private JsonResponse proceedToLogin(final Subject currentUser, final AuthenticationToken token) {
    JsonResponse response = null;
    try {
      logoutCurrentUser();
      currentUser.getSession(true);
      currentUser.login(token);

      final Set<String> roles = securityService.getAssociatedRoles();
      final String principal = securityService.getPrincipal();
      final String ticket;
      if ("anonymous".equals(principal)) {
        ticket = "anonymous";
      } else {
        ticket = TicketContainer.instance.getTicket(principal);
      }

      final Map<String, String> data = new HashMap<>();
      data.put("principal", principal);
      data.put("roles", gson.toJson(roles));
      data.put("ticket", ticket);

      response = new JsonResponse(HttpStatus.OK, "", data);
      // if no exception, that's it, we're done!
    } catch (final AuthenticationException uae) {
      // username wasn't in the system, show them an error message?
      // password didn't match, try again?
      // account for that username is locked - can't login.  Show them a message?
      // unexpected condition - error?
      LOG.error("Exception in login: ", uae);
    }
    return response;
  }

  /**
   * Post Login
   * Returns userName & password
   * for anonymous access, username is always anonymous.
   * After getting this ticket, access through websockets become safe
   *
   * @return 200 response
   */
  @PostMapping(produces = "application/json")
  public ResponseEntity postLogin(@RequestParam("userName") final String userName,
                                  @RequestParam("password") final String password) {
    LOG.debug("userName:" + userName);
    JsonResponse response = null;
    // ticket set to anonymous for anonymous user. Simplify testing.
    final Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
    if (currentUser.isAuthenticated()) {
      currentUser.logout();
    }
    LOG.debug("currentUser: " + currentUser);
    if (!currentUser.isAuthenticated()) {

      final UsernamePasswordToken token = new UsernamePasswordToken(userName, password);

      response = proceedToLogin(currentUser, token);
    }

    if (response == null) {
      response = new JsonResponse(HttpStatus.FORBIDDEN, "", "");
    }

    LOG.warn(response.toString());
    return response.build();
  }

  @PostMapping(value = "/logout", produces = "application/json")
  public ResponseEntity logout() {
    final JsonResponse response;
    logoutCurrentUser();
    HttpStatus status = null;
    final Map<String, String> data = new HashMap<>();

    response = new JsonResponse(status, "", data);

    LOG.warn(response.toString());
    return response.build();
  }


  private void logoutCurrentUser() {
    final Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
    TicketContainer.instance.removeTicket(securityService.getPrincipal());
    currentUser.getSession().stop();
    currentUser.logout();
  }
}
