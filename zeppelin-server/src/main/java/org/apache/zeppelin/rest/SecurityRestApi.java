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
import org.apache.shiro.SecurityUtils;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

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
    return response.build();
  }

  /**
   * Get userlist.
   *
   * Returns list of all user from available realms
   *
   * @return 200 response
   */
  @GetMapping(value = "/userlist/{searchText}", produces = "application/json")
  public ResponseEntity getUserList(@PathVariable("searchText") final String searchText) {


    /*final int numUsersToFetch = 5;
    final List<String> usersList = securityService.getMatchedUsers(searchText, numUsersToFetch);
    final List<String> rolesList = securityService.getMatchedRoles();

*/
    final List<String> autoSuggestUserList = new ArrayList<>();
    final List<String> autoSuggestRoleList = new ArrayList<>();

    /*
    Collections.sort(usersList);
    Collections.sort(rolesList);
    usersList.sort((o1, o2) -> {
      if (o1.matches(searchText + "(.*)") && o2.matches(searchText + "(.*)")) {
        return 0;
      } else if (o1.matches(searchText + "(.*)")) {
        return -1;
      }
      return 0;
    });
    int maxLength = 0;
    for (final String user : usersList) {
      if (StringUtils.containsIgnoreCase(user, searchText)) {
        autoSuggestUserList.add(user);
        maxLength++;
      }
      if (maxLength == numUsersToFetch) {
        break;
      }
    }

    for (final String role : rolesList) {
      if (StringUtils.containsIgnoreCase(role, searchText)) {
        autoSuggestRoleList.add(role);
      }
    }
    */
    final Map<String, List> returnListMap = new HashMap<>();
    returnListMap.put("users", autoSuggestUserList);
    returnListMap.put("roles", autoSuggestRoleList);

    return new JsonResponse(HttpStatus.OK, "", returnListMap).build();

  }
}
