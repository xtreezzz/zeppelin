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

package org.apache.zeppelin.notebook;

import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.apache.zeppelin.storage.ConfigStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * Contains authorization information for notes
 */
@Component
public class NotebookAuthorization {
  private static final Logger LOG = LoggerFactory.getLogger(NotebookAuthorization.class);
  /*
   * { "note1": { "owners": ["u1"], "readers": ["u1", "u2"], "runners": ["u2"],
   * "writers": ["u1"] },  "note2": ... } }
   */
  private final Map<String, Map<String, Set<String>>> authInfo = new HashMap<>();

  private ConfigStorage configStorage;
  private final ZeppelinConfiguration conf;

  @Autowired
  public NotebookAuthorization(final ZeppelinConfiguration zeppelinConfiguration) {
    this.conf = zeppelinConfiguration;

    try {
      configStorage = ConfigStorage.getInstance(conf);
      loadFromFile();
    } catch (final IOException e) {
      LOG.error("Error loading NotebookAuthorization", e);
    }
  }

  private void loadFromFile() throws IOException {
    final NotebookAuthorizationInfoSaving info = configStorage.loadNotebookAuthorization();
    if (info != null) {
      authInfo.clear();
      authInfo.putAll(info.authInfo);
    }
  }

  private void saveToFile() {
    synchronized (authInfo) {
      final NotebookAuthorizationInfoSaving info = new NotebookAuthorizationInfoSaving();
      info.authInfo = authInfo;
      try {
        configStorage.save(info);
      } catch (final IOException e) {
        LOG.error("Error saving notebook authorization file", e);
      }
    }
  }

  private Set<String> validateUser(final Set<String> users) {
    final Set<String> returnUser = new HashSet<>();
    for (final String user : users) {
      if (!user.trim().isEmpty()) {
        returnUser.add(user.trim());
      }
    }
    return returnUser;
  }

  public void setOwners(final String noteId, Set<String> entities) {
    Map<String, Set<String>> noteAuthInfo = authInfo.get(noteId);
    entities = validateUser(entities);
    if (noteAuthInfo == null) {
      noteAuthInfo = new LinkedHashMap<>();
      noteAuthInfo.put("owners", new LinkedHashSet<>(entities));
      noteAuthInfo.put("readers", new LinkedHashSet<>());
      noteAuthInfo.put("runners", new LinkedHashSet<>());
      noteAuthInfo.put("writers", new LinkedHashSet<>());
    } else {
      noteAuthInfo.put("owners", new LinkedHashSet<>(entities));
    }
    authInfo.put(noteId, noteAuthInfo);
    saveToFile();
  }

  public void setReaders(final String noteId, Set<String> entities) {
    Map<String, Set<String>> noteAuthInfo = authInfo.get(noteId);
    entities = validateUser(entities);
    if (noteAuthInfo == null) {
      noteAuthInfo = new LinkedHashMap<>();
      noteAuthInfo.put("owners", new LinkedHashSet<>());
      noteAuthInfo.put("readers", new LinkedHashSet<>(entities));
      noteAuthInfo.put("runners", new LinkedHashSet<>());
      noteAuthInfo.put("writers", new LinkedHashSet<>());
    } else {
      noteAuthInfo.put("readers", new LinkedHashSet<>(entities));
    }
    authInfo.put(noteId, noteAuthInfo);
    saveToFile();
  }

  public void setRunners(final String noteId, Set<String> entities) {
    Map<String, Set<String>> noteAuthInfo = authInfo.get(noteId);
    entities = validateUser(entities);
    if (noteAuthInfo == null) {
      noteAuthInfo = new LinkedHashMap<>();
      noteAuthInfo.put("owners", new LinkedHashSet<>());
      noteAuthInfo.put("readers", new LinkedHashSet<>());
      noteAuthInfo.put("runners", new LinkedHashSet<>(entities));
      noteAuthInfo.put("writers", new LinkedHashSet<>());
    } else {
      noteAuthInfo.put("runners", new LinkedHashSet<>(entities));
    }
    authInfo.put(noteId, noteAuthInfo);
    saveToFile();
  }


  public void setWriters(final String noteId, Set<String> entities) {
    Map<String, Set<String>> noteAuthInfo = authInfo.get(noteId);
    entities = validateUser(entities);
    if (noteAuthInfo == null) {
      noteAuthInfo = new LinkedHashMap<>();
      noteAuthInfo.put("owners", new LinkedHashSet<>());
      noteAuthInfo.put("readers", new LinkedHashSet<>());
      noteAuthInfo.put("runners", new LinkedHashSet<>());
      noteAuthInfo.put("writers", new LinkedHashSet<>(entities));
    } else {
      noteAuthInfo.put("writers", new LinkedHashSet<>(entities));
    }
    authInfo.put(noteId, noteAuthInfo);
    saveToFile();
  }

  /*
   * If case conversion is enforced, then change entity names to lower case
   */
  private Set<String> checkCaseAndConvert(final Set<String> entities) {
    if (conf.isUsernameForceLowerCase()) {
      final Set<String> set2 = new HashSet<>();
      for (final String name : entities) {
        set2.add(name.toLowerCase());
      }
      return set2;
    } else {
      return entities;
    }
  }

  public Set<String> getOwners(final String noteId) {
    final Map<String, Set<String>> noteAuthInfo = authInfo.get(noteId);
    Set<String> entities;
    if (noteAuthInfo == null) {
      entities = new HashSet<>();
    } else {
      entities = noteAuthInfo.get("owners");
      if (entities == null) {
        entities = new HashSet<>();
      } else {
        entities = checkCaseAndConvert(entities);
      }
    }
    return entities;
  }

  public Set<String> getReaders(final String noteId) {
    final Map<String, Set<String>> noteAuthInfo = authInfo.get(noteId);
    Set<String> entities;
    if (noteAuthInfo == null) {
      entities = new HashSet<>();
    } else {
      entities = noteAuthInfo.get("readers");
      if (entities == null) {
        entities = new HashSet<>();
      } else {
        entities = checkCaseAndConvert(entities);
      }
    }
    return entities;
  }

  public Set<String> getRunners(final String noteId) {
    final Map<String, Set<String>> noteAuthInfo = authInfo.get(noteId);
    Set<String> entities;
    if (noteAuthInfo == null) {
      entities = new HashSet<>();
    } else {
      entities = noteAuthInfo.get("runners");
      if (entities == null) {
        entities = new HashSet<>();
      } else {
        entities = checkCaseAndConvert(entities);
      }
    }
    return entities;
  }

  public Set<String> getWriters(final String noteId) {
    final Map<String, Set<String>> noteAuthInfo = authInfo.get(noteId);
    Set<String> entities;
    if (noteAuthInfo == null) {
      entities = new HashSet<>();
    } else {
      entities = noteAuthInfo.get("writers");
      if (entities == null) {
        entities = new HashSet<>();
      } else {
        entities = checkCaseAndConvert(entities);
      }
    }
    return entities;
  }

  public boolean isOwner(final String noteId, final Set<String> entities) {
    return isMember(entities, getOwners(noteId)) || isAdmin(entities);
  }

  public boolean isWriter(final String noteId, final Set<String> entities) {
    return isMember(entities, getWriters(noteId)) ||
            isMember(entities, getOwners(noteId)) ||
            isAdmin(entities);
  }

  public boolean isReader(final String noteId, final Set<String> entities) {
    return isMember(entities, getReaders(noteId)) ||
            isMember(entities, getOwners(noteId)) ||
            isMember(entities, getWriters(noteId)) ||
            isMember(entities, getRunners(noteId)) ||
            isAdmin(entities);
  }

  public boolean isRunner(final String noteId, final Set<String> entities) {
    return isMember(entities, getRunners(noteId)) ||
            isMember(entities, getWriters(noteId)) ||
            isMember(entities, getOwners(noteId)) ||
            isAdmin(entities);
  }

  private boolean isAdmin(final Set<String> entities) {
    final String adminRole = conf.getString(ConfVars.ZEPPELIN_OWNER_ROLE);
    if (StringUtils.isBlank(adminRole)) {
      return false;
    }
    return entities.contains(adminRole);
  }

  // return true if b is empty or if (a intersection b) is non-empty
  private boolean isMember(final Set<String> a, final Set<String> b) {
    final Set<String> intersection = new HashSet<>(b);
    intersection.retainAll(a);
    return (b.isEmpty() || (intersection.size() > 0));
  }

  public boolean isOwner(final Set<String> userAndRoles, final String noteId) {
    if (conf.isAnonymousAllowed()) {
      LOG.debug("Zeppelin runs in anonymous mode, everybody is owner");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isOwner(noteId, userAndRoles);
  }

  public boolean hasWriteAuthorization(final Set<String> userAndRoles, final String noteId) {
    if (conf.isAnonymousAllowed()) {
      LOG.debug("Zeppelin runs in anonymous mode, everybody is writer");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isWriter(noteId, userAndRoles);
  }

  public boolean hasReadAuthorization(final Set<String> userAndRoles, final String noteId) {
    if (conf.isAnonymousAllowed()) {
      LOG.debug("Zeppelin runs in anonymous mode, everybody is reader");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isReader(noteId, userAndRoles);
  }

  public boolean hasRunAuthorization(final Set<String> userAndRoles, final String noteId) {
    if (conf.isAnonymousAllowed()) {
      LOG.debug("Zeppelin runs in anonymous mode, everybody is runner");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isRunner(noteId, userAndRoles);
  }

  public void removeNote(final String noteId) {
    authInfo.remove(noteId);
    saveToFile();
  }

  public void setNewNotePermissions(final String noteId, final String user) {
    if (conf.isNotebookPublic()) {
      // add current user to owners - can be public
      final Set<String> owners = getOwners(noteId);
      owners.add(user);
      setOwners(noteId, owners);
    } else {
      // add current user to owners, readers, runners, writers - private note
      Set<String> entities = getOwners(noteId);
      entities.add(user);
      setOwners(noteId, entities);
      entities = getReaders(noteId);
      entities.add(user);
      setReaders(noteId, entities);
      entities = getRunners(noteId);
      entities.add(user);
      setRunners(noteId, entities);
      entities = getWriters(noteId);
      entities.add(user);
      setWriters(noteId, entities);
    }
  }
}
