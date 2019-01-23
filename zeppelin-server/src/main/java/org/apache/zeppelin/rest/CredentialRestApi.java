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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.SecurityService;
import org.apache.zeppelin.user.Credentials;
import org.apache.zeppelin.user.UserCredentials;
import org.apache.zeppelin.user.UsernamePassword;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

/** Credential Rest API. */
//@Path("/credential")
//@Produces("application/json")
//@Singleton
@RestController
@RequestMapping("/api/credential")
public class CredentialRestApi {
  Logger logger = LoggerFactory.getLogger(CredentialRestApi.class);
  private final Credentials credentials;
  private final SecurityService securityService;
  private final Gson gson = new Gson();

  @Autowired
  public CredentialRestApi(final Credentials credentials, @Qualifier("NoSecurityService") final SecurityService securityService) {
    this.credentials = credentials;
    this.securityService = securityService;
  }

  /**
   * Put User Credentials REST API.
   *
   * @param message - JSON with entity, username, password.
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  //@PUT
  @PutMapping(produces = "application/json")
  public ResponseEntity putCredentials(final String message) throws IOException, IllegalArgumentException {
    final Map<String, String> messageMap =
        gson.fromJson(message, new TypeToken<Map<String, String>>() {}.getType());
    final String entity = messageMap.get("entity");
    final String username = messageMap.get("username");
    final String password = messageMap.get("password");

    if (Strings.isNullOrEmpty(entity)
        || Strings.isNullOrEmpty(username)
        || Strings.isNullOrEmpty(password)) {
      return new JsonResponse(HttpStatus.BAD_REQUEST).build();
    }

    final String user = securityService.getPrincipal();
    logger.info("Update credentials for user {} entity {}", user, entity);
    final UserCredentials uc = credentials.getUserCredentials(user);
    uc.putUsernamePassword(entity, new UsernamePassword(username, password));
    credentials.putUserCredentials(user, uc);
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Get User Credentials list REST API.
   *
   * @return JSON with status.OK
   * @throws IllegalArgumentException
   */
  //@GET
  @GetMapping(produces = "application/json")
  public ResponseEntity getCredentials() throws IllegalArgumentException {
    final String user = securityService.getPrincipal();
    logger.info("getCredentials credentials for user {} ", user);
    final UserCredentials uc = credentials.getUserCredentials(user);
    return new JsonResponse(HttpStatus.OK, uc).build();
  }

  /**
   * Remove User Credentials REST API.
   *
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  //@DELETE
  @DeleteMapping(produces = "application/json")
  public ResponseEntity removeCredentials() throws IOException, IllegalArgumentException {
    final String user = securityService.getPrincipal();
    logger.info("removeCredentials credentials for user {} ", user);
    final UserCredentials uc = credentials.removeUserCredentials(user);
    if (uc == null) {
      return new JsonResponse(HttpStatus.NOT_FOUND).build();
    }
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Remove Entity of User Credential entity REST API.
   *
   * @param
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
 // @DELETE
 // @Path("{entity}")
  @DeleteMapping(value = "/{entity}", produces = "application/json")
  public ResponseEntity removeCredentialEntity(@PathVariable("entity") final String entity)
      throws IOException, IllegalArgumentException {
    final String user = securityService.getPrincipal();
    logger.info("removeCredentialEntity for user {} entity {}", user, entity);
    if (!credentials.removeCredentialEntity(user, entity)) {
      return new JsonResponse(HttpStatus.NOT_FOUND).build();
    }
    return new JsonResponse(HttpStatus.OK).build();
  }
}
