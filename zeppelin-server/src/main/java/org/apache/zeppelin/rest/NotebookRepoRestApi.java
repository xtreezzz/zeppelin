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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.notebook.repo.NotebookRepoSync;
import org.apache.zeppelin.notebook.repo.NotebookRepoWithSettings;
import org.apache.zeppelin.rest.message.NotebookRepoSettingsRequest;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.SecurityService;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * NoteRepo rest API endpoint.
 *
 */
//@Path("/notebook-repositories")
//@Produces("application/json")
//@Singleton
@RestController
@RequestMapping("/api/notebook-repositories")
public class NotebookRepoRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(NotebookRepoRestApi.class);

  private final NotebookRepoSync noteRepos;
  private final SecurityService securityService;

  @Autowired
  public NotebookRepoRestApi(final NotebookRepoSync noteRepos,
                             @Qualifier("NoSecurityService") final SecurityService securityService) {
    this.noteRepos = noteRepos;
    this.securityService = securityService;
  }

  /**
   * List all notebook repository.
   */
  //@GET
  @ZeppelinApi
  @GetMapping( produces = "application/json")
  public ResponseEntity listRepoSettings() {
    final AuthenticationInfo subject = new AuthenticationInfo(securityService.getPrincipal());
    LOG.info("Getting list of NoteRepo with Settings for user {}", subject.getUser());
    final List<NotebookRepoWithSettings> settings = noteRepos.getNotebookRepos(subject);
    return new JsonResponse(HttpStatus.OK, "", settings).build();
  }

  /**
   * Reload notebook repository.
   */
  //@GET
  //@Path("reload")
  @ZeppelinApi
  @GetMapping(value = "/reload", produces = "application/json")
  public ResponseEntity refreshRepo(){
    final AuthenticationInfo subject = new AuthenticationInfo(securityService.getPrincipal());
    LOG.info("Reloading notebook repository for user {}", subject.getUser());
    //TODO(KOT): FIX
      /*
    try {
      notebookWsServer.broadcastReloadedNoteList(null, getServiceContext());
    } catch (IOException e) {
      LOG.error("Fail to refresh repo", e);
    }
    */
    return new JsonResponse(HttpStatus.OK, "", null).build();
  }

  private ServiceContext getServiceContext() {
    final AuthenticationInfo authInfo = new AuthenticationInfo(securityService.getPrincipal());
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(securityService.getPrincipal());
    userAndRoles.addAll(securityService.getAssociatedRoles());
    return new ServiceContext(authInfo, userAndRoles);
  }

  /**
   * Update a specific note repo.
   *
   * @param payload
   * @return
   */
  //@PUT
  @ZeppelinApi
  @PutMapping(produces = "application/json")
  public ResponseEntity updateRepoSetting(final String payload) {
    if (StringUtils.isBlank(payload)) {
      return new JsonResponse(HttpStatus.NOT_FOUND, "", Collections.emptyMap()).build();
    }
    final AuthenticationInfo subject = new AuthenticationInfo(securityService.getPrincipal());
    final NotebookRepoSettingsRequest newSettings;
    try {
      newSettings = NotebookRepoSettingsRequest.fromJson(payload);
    } catch (final JsonSyntaxException e) {
      LOG.error("Cannot update notebook repo settings", e);
      return new JsonResponse(HttpStatus.NOT_ACCEPTABLE, "",
          ImmutableMap.of("error", "Invalid payload structure")).build();
    }

    if (NotebookRepoSettingsRequest.isEmpty(newSettings)) {
      LOG.error("Invalid property");
      return new JsonResponse(HttpStatus.NOT_ACCEPTABLE, "",
          ImmutableMap.of("error", "Invalid payload")).build();
    }
    LOG.info("User {} is going to change repo setting", subject.getUser());
    final NotebookRepoWithSettings updatedSettings =
        noteRepos.updateNotebookRepo(newSettings.name, newSettings.settings, subject);
    if (!updatedSettings.isEmpty()) {
      LOG.info("Broadcasting note list to user {}", subject.getUser());
      //TODO(KOT): FIX
      /*
      try {
        notebookWsServer.broadcastReloadedNoteList(null, getServiceContext());
      } catch (IOException e) {
        LOG.error("Fail to refresh repo.", e);
      } */
    }
    return new JsonResponse(HttpStatus.OK, "", updatedSettings).build();
  }
}
