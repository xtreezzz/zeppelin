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
import com.google.gson.GsonBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Json response builder.
 */
public class JsonResponse {
  private HttpStatus status;
  private String message;
  private final Object body;

  public JsonResponse(final HttpStatus status) {
    this.status = status;
    this.message = null;
    this.body = null;
  }

  public JsonResponse(final HttpStatus status, final String message) {
    this.status = status;
    this.message = message;
    this.body = null;
  }

  public JsonResponse(final HttpStatus status, final Object body) {
    this.status = status;
    this.message = null;
    this.body = body;
  }

  public JsonResponse(final HttpStatus status, final String message, final Object body) {
    this.status = status;
    this.message = message;
    this.body = body;
  }

  public HttpStatus getCode() {
    return status;
  }

  public void setCode(final HttpStatus status) {
    this.status = status;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(final String message) {
    this.message = message;
  }

  public ResponseEntity build() {
    return new ResponseEntity<>(this.toString(), status);
  }

  @Override
  public String toString() {
    final GsonBuilder gsonBuilder = new GsonBuilder();
    final Gson gson = gsonBuilder.create();
    return gson.toJson(this);
  }
}
