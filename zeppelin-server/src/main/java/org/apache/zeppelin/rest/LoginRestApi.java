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
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created for org.apache.zeppelin.rest.message.
 */
@RestController
@RequestMapping("/api/login")
public class LoginRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(LoginRestApi.class);
  private static final Gson gson = new Gson();


  @GetMapping(produces = "application/json")
  public ResponseEntity getLogin(final HttpServletRequest request) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Map<String, String> data = new HashMap<>();
    data.put("principal", authenticationInfo.getUser());
    data.put("roles", gson.toJson(authenticationInfo.getRoles()));
    data.put("ticket", UUID.randomUUID().toString());

    return new JsonResponse(HttpStatus.OK, "", data).build();
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
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Map<String, String> data = new HashMap<>();
    data.put("principal", authenticationInfo.getUser());
    data.put("roles", gson.toJson(authenticationInfo.getRoles()));
    data.put("ticket", UUID.randomUUID().toString());

    return new JsonResponse(HttpStatus.OK, "", data).build();
  }

  @PostMapping(value = "/logout", produces = "application/json")
  public ResponseEntity logout() {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    AuthorizationService.drop(authenticationInfo.getUser());

    Map<String, String> data = new HashMap<>();
    data.put("clearAuthorizationHeader", "true");
    return new JsonResponse(HttpStatus.UNAUTHORIZED, "", data).build();
  }
}
