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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.cache.CacheManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/cache")
public class CacheRestApi {

  private final CacheManager cacheManager;

  public CacheRestApi(final CacheManager cacheManager) {
    this.cacheManager = cacheManager;
  }

  @GetMapping(value = "/names", produces = "application/json")
  public ResponseEntity listCacheNames() {
    try {
      return new JsonResponse(HttpStatus.OK, "", cacheManager.getCacheNames()).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @PostMapping(value = "/evict/{cacheName}", produces = "application/json")
  public ResponseEntity evictCache(final @PathVariable String cacheName) {
    try {
      cacheManager.getCache(cacheName).clear();
      return new JsonResponse(HttpStatus.OK, "").build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
