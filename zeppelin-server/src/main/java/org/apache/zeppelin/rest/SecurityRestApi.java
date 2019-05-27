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

import com.google.gson.Gson;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.web.mgt.DefaultWebSecurityManager;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.realm.ShiroSecurityService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;

/**
 * Zeppelin security rest api endpoint.
 */
@RestController
@RequestMapping("/api/security")
public class SecurityRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityRestApi.class);
  private static final Gson gson = new Gson();

  /**
   * Get ticket
   * Returns username & ticket
   * for anonymous access, username is always anonymous.
   * After getting this ticket, access through websockets become safe
   *
   * @return 200 response
   */
  @GetMapping(value = "/ticket", produces = "application/json")
  public ResponseEntity ticket() {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Map<String, String> data = new HashMap<>();
    data.put("principal", authenticationInfo.getUser());
    data.put("roles", gson.toJson(authenticationInfo.getRoles()));
    data.put("ticket", UUID.randomUUID().toString());

    final JsonResponse response = new JsonResponse(HttpStatus.OK, "", data);
    LOG.warn(response.toString());

    ZLog.log(ET.USER_CONNECTED, "Пользователь вошел в систему",
        "Пользователь получил тикет с помощью GET: /api/security/ticket",
        authenticationInfo.getUser());
    return response.build();
  }

  /**
   * Get userlist.
   * <p>
   * Returns list of all user from available realms
   *
   * @return 200 response
   */
  @GetMapping(value = "/userlist/{searchText}", produces = "application/json")
  public ResponseEntity getUserList(@PathVariable("searchText") final String searchText) {

    final Map<String, Set<String>> emptyResultObj = new HashMap<>();
    emptyResultObj.put("users", new HashSet<>());
    emptyResultObj.put("roles", new HashSet<>());
    final ResponseEntity emptyResult = new JsonResponse(HttpStatus.OK, "", emptyResultObj).build();

    final int numUsersToFetch = 5;
    final Set<String> usersList = new HashSet<>();
    final Set<String> rolesList = new HashSet<>();
    // get security manager
    final DefaultWebSecurityManager securityManager = (DefaultWebSecurityManager) SecurityUtils.getSecurityManager();
    if(securityManager == null) {
      return emptyResult;
    }
    final Collection<Realm> realms = securityManager.getRealms();
    if(realms == null || realms.isEmpty()) {
      return emptyResult;
    }

    for (final Realm realm : realms) {
      final ShiroSecurityService securityService = (ShiroSecurityService) realm;
      usersList.addAll(securityService.getMatchedUsers(searchText, numUsersToFetch));
      rolesList.addAll(securityService.getMatchedRoles(searchText, numUsersToFetch));
    }

    final Map<String, Set<String>> returnListMap = new HashMap<>();
    returnListMap.put("users", usersList);
    returnListMap.put("roles", rolesList);

    return new JsonResponse(HttpStatus.OK, "", returnListMap).build();

  }
}
