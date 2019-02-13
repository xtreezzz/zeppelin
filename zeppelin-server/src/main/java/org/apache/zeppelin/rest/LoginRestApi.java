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
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.Subject;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.NotebookAuthorization;
import org.apache.zeppelin.realm.jwt.JWTAuthenticationToken;
import org.apache.zeppelin.realm.jwt.KnoxJwtRealm;
import org.apache.zeppelin.realm.kerberos.KerberosRealm;
import org.apache.zeppelin.realm.kerberos.KerberosToken;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.SecurityService;
import org.apache.zeppelin.ticket.TicketContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.text.ParseException;
import java.util.*;

/**
 * Created for org.apache.zeppelin.rest.message.
 */
@RestController
@RequestMapping("/api/login")
public class LoginRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(LoginRestApi.class);
  private static final Gson gson = new Gson();
  private final ZeppelinConfiguration zConf;
  private final SecurityService securityService;
  private final NotebookAuthorization notebookAuthorization;

  @Autowired
  public LoginRestApi(final Notebook notebook,
                      @Qualifier("NoSecurityService") final SecurityService securityService) {
    this.zConf = notebook.getConf();
    this.securityService = securityService;
    this.notebookAuthorization = notebook.getNotebookAuthorization();
  }

  @ZeppelinApi
  @GetMapping(produces = "application/json")
  public ResponseEntity getLogin(final HttpServletRequest request) {
    JsonResponse response = null;
    final Map<String, Cookie> cookieMap = Maps.uniqueIndex(Arrays.asList(request.getCookies()), Cookie::getName);
    if (isKnoxSSOEnabled()) {
      final KnoxJwtRealm knoxJwtRealm = getJTWRealm();
      final Cookie cookie = cookieMap.get(knoxJwtRealm.getCookieName());
      if (cookie != null && cookie.getValue() != null) {
        final Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
        final JWTAuthenticationToken token = new JWTAuthenticationToken(null, cookie.getValue());
        try {
          final String name = knoxJwtRealm.getName(token);
          if (!currentUser.isAuthenticated() || !currentUser.getPrincipal().equals(name)) {
            response = proceedToLogin(currentUser, token);
          }
        } catch (final ParseException e) {
          LOG.error("ParseException in LoginRestApi: ", e);
        }
      }
      if (response == null) {
        final Map<String, String> data = new HashMap<>();
        data.put("redirectURL", constructKnoxUrl(knoxJwtRealm, knoxJwtRealm.getLogin()));
        response = new JsonResponse(HttpStatus.OK, "", data);
      }
      return response.build();
    } else {
      final KerberosRealm kerberosRealm = getKerberosRealm();
      if (null != kerberosRealm) {
        try {
          final KerberosToken kerberosToken = KerberosRealm.getKerberosTokenFromCookies(cookieMap);
          if (null != kerberosToken) {
            final Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
            final String name = (String) kerberosToken.getPrincipal();
            if (!currentUser.isAuthenticated() || !currentUser.getPrincipal().equals(name)) {
              response = proceedToLogin(currentUser, kerberosToken);
            }
          }
          if (null == response) {
            LOG.warn("No Kerberos token received");
            response = new JsonResponse(HttpStatus.UNAUTHORIZED, "", null);
          }
          return response.build();
        } catch (final AuthenticationException e){
          LOG.error("Error in Login: " + e);
        }
      }
    }
    return new JsonResponse(HttpStatus.METHOD_NOT_ALLOWED).build();
  }

  private KerberosRealm getKerberosRealm() {
    final Collection realmsList = securityService.getRealmsList();
    if (realmsList != null) {
      for (final Iterator<Realm> iterator = realmsList.iterator(); iterator.hasNext(); ) {
        final Realm realm = iterator.next();
        final String name = realm.getClass().getName();

        LOG.debug("RealmClass.getName: " + name);

        if (name.equals("org.apache.zeppelin.realm.kerberos.KerberosRealm")) {
          return (KerberosRealm) realm;
        }
      }
    }
    return null;
  }

  private KnoxJwtRealm getJTWRealm() {
    final Collection realmsList = securityService.getRealmsList();
    if (realmsList != null) {
      for (final Iterator<Realm> iterator = realmsList.iterator(); iterator.hasNext(); ) {
        final Realm realm = iterator.next();
        final String name = realm.getClass().getName();

        LOG.debug("RealmClass.getName: " + name);

        if (name.equals("org.apache.zeppelin.realm.jwt.KnoxJwtRealm")) {
          return (KnoxJwtRealm) realm;
        }
      }
    }
    return null;
  }

  private boolean isKnoxSSOEnabled() {
    final Collection realmsList = securityService.getRealmsList();
    if (realmsList != null) {
      for (final Iterator<Realm> iterator = realmsList.iterator(); iterator.hasNext(); ) {
        final Realm realm = iterator.next();
        final String name = realm.getClass().getName();
        LOG.debug("RealmClass.getName: " + name);
        if (name.equals("org.apache.zeppelin.realm.jwt.KnoxJwtRealm")) {
          return true;
        }
      }
    }
    return false;
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

      // set roles for user in NotebookAuthorization module
      notebookAuthorization.setRoles(principal, roles);
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
  @ZeppelinApi
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

  @ZeppelinApi
  @PostMapping(value = "/logout", produces = "application/json")
  public ResponseEntity logout() {
    final JsonResponse response;
    logoutCurrentUser();
    HttpStatus status = null;
    final Map<String, String> data = new HashMap<>();
    if (zConf.isAuthorizationHeaderClear()) {
      status = HttpStatus.UNAUTHORIZED;
      data.put("clearAuthorizationHeader", "true");
    } else {
      status = HttpStatus.FORBIDDEN;
      data.put("clearAuthorizationHeader", "false");
    }
    if (isKnoxSSOEnabled()) {
      final KnoxJwtRealm knoxJwtRealm = getJTWRealm();
      data.put("redirectURL", constructKnoxUrl(knoxJwtRealm, knoxJwtRealm.getLogout()));
      data.put("isLogoutAPI", knoxJwtRealm.getLogoutAPI().toString());
      response = new JsonResponse(status, "", data);
    } else {
      response = new JsonResponse(status, "", data);
    }
    LOG.warn(response.toString());
    return response.build();
  }

  private String constructKnoxUrl(final KnoxJwtRealm knoxJwtRealm, final String path) {
    final StringBuilder redirectURL = new StringBuilder(knoxJwtRealm.getProviderUrl());
    redirectURL.append(path);
    if (knoxJwtRealm.getRedirectParam() != null) {
      redirectURL.append("?").append(knoxJwtRealm.getRedirectParam()).append("=");
    }
    return redirectURL.toString();
  }

  private void logoutCurrentUser() {
    final Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
    TicketContainer.instance.removeTicket(securityService.getPrincipal());
    currentUser.getSession().stop();
    currentUser.logout();
  }
}
