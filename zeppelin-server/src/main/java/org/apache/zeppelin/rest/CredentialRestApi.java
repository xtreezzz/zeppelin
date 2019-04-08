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

import org.apache.zeppelin.server.JsonResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/** Credential Rest API. */
@RestController
@RequestMapping("/api/credential")
public class CredentialRestApi {

  @Autowired
  public CredentialRestApi() {
  }

  /**
   * Put User Credentials REST API.
   */
  @PutMapping(produces = "application/json")
  public ResponseEntity putCredentials(final String message) throws IOException, IllegalArgumentException {
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Get User Credentials list REST API.
   */
  @GetMapping(produces = "application/json")
  public ResponseEntity getCredentials() throws IllegalArgumentException {
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Remove User Credentials REST API.
   */
  @DeleteMapping(produces = "application/json")
  public ResponseEntity removeCredentials() throws IOException, IllegalArgumentException {
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Remove Entity of User Credential entity REST API.
   */
  @DeleteMapping(value = "/{entity}", produces = "application/json")
  public ResponseEntity removeCredentialEntity(@PathVariable("entity") final String entity)
      throws IOException, IllegalArgumentException {
    return new JsonResponse(HttpStatus.OK).build();
  }
}
