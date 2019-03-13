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
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.apache.shiro.SecurityUtils;
import org.apache.zeppelin.repositories.DatabaseNoteRepository;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/favorite_notes")
public class FavoriteNotesRestApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(FavoriteNotesRestApi.class);

  private static final Integer RECENT_NOTEBOOK_COUNT = 7;
  private static final Integer SAVE_ON_DISK_INTERVAL = 5; // minutes

  private final ZeppelinConfiguration configuration;
  private final DatabaseNoteRepository noteRepository;
  private final ScheduledExecutorService saveExecServ;

  private File dataFile;
  private Map<String, Map<String, Set<String>>> usersNotes;

  @Autowired
  public FavoriteNotesRestApi(ZeppelinConfiguration configuration, DatabaseNoteRepository noteRepository) {
    this.configuration = configuration;
    this.saveExecServ = Executors.newSingleThreadScheduledExecutor();
    this.noteRepository = noteRepository;
  }

  @PostConstruct
  public void init() {
    String filePath = configuration.getFavoriteNotesFilePath();
    dataFile = new File(filePath);
    usersNotes = loadNotesList();
    clearIncorrectNotesIds();

    Runtime.getRuntime().addShutdownHook(new Thread(this::saveNotesList));
    saveExecServ.scheduleWithFixedDelay(this::saveNotesList, SAVE_ON_DISK_INTERVAL, SAVE_ON_DISK_INTERVAL, TimeUnit.MINUTES);
  }

  private synchronized ConcurrentHashMap<String, Map<String, Set<String>>> loadNotesList() {
    Type type = new TypeToken<ConcurrentHashMap<String, HashMap<String, LinkedHashSet<String>>>>() {
    }.getType();
    try (FileReader fileReader = new FileReader(dataFile)) {
      Gson gson = new Gson();
      JsonReader reader = new JsonReader(fileReader);
      return gson.fromJson(reader, type);
    } catch (IOException e) {
      LOGGER.info("Json file with favorites notes not found!");
      return new ConcurrentHashMap<>();
    }
  }

  private synchronized void saveNotesList() {
    clearIncorrectNotesIds();
    try (FileWriter fileWriter = new FileWriter(dataFile)) {
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      gson.toJson(usersNotes, fileWriter);
    } catch (IOException e) {
      LOGGER.error("Error when try save 'favorites notes list file'", e);
    }
  }

  private boolean noteIdIsCorrect(String noteId) {
    Note note = noteRepository.getNote(noteId);
    if (note == null) {
      return false;
    }
    return !note.isTrashed();
  }

  private void clearIncorrectNotesIds() {
    for (Map<String, Set<String>> userList : usersNotes.values()) {
      for (Set<String> noteType : userList.values()) {
        noteType.removeIf(id -> !noteIdIsCorrect(id));
      }
    }
  }

  @ZeppelinApi
  @GetMapping(value = "/get_notes_ids", produces = "application/json")
  public ResponseEntity getNotesIds(@RequestParam("username") String username) {
    try {
      HashMap<String, Set> idsMap = new HashMap<>(2);
      Set<String> favoriteSet = Collections.emptySet();
      Set<String> recentSet = Collections.emptySet();
      try {
        favoriteSet = Objects.requireNonNull(usersNotes.get(username).get("favorite"));
      } catch (Exception ignore) {
      }
      try {
        recentSet = Objects.requireNonNull(usersNotes.get(username).get("recent"));
      } catch (Exception ignore) {
      }
      idsMap.put("favorite", favoriteSet);
      idsMap.put("recent", recentSet);
      return new JsonResponse(HttpStatus.OK, "", idsMap).build();
    } catch (NullPointerException ignore) {
      return new JsonResponse(HttpStatus.OK, "", Collections.emptyList()).build();
    }
  }

  @ZeppelinApi
//  @GetMapping(value = "/set_note_status", produces = "application/json")
  public ResponseEntity setNoteStatus(@RequestParam("username") String username,
                                      @RequestParam("note_id") String noteId,
                                      @RequestParam("note_type") String noteType,
                                      @RequestParam("note_action") String noteAction) {

    if (username == null || username.isEmpty() || !SecurityUtils.getSubject().getPrincipal().equals(username)) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "username no correct").build();
    }
    if (noteId == null || noteId.isEmpty()) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "note_id no correct").build();
    }
    if (noteType == null || noteType.isEmpty() || !(noteType.equals("favorite") || noteType.equals("recent"))) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "note_type no correct").build();
    }
    if (noteAction == null || noteAction.isEmpty() || !(noteAction.equals("add") || noteAction.equals("remove"))) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "note_action no correct").build();
    }

    Map<String, Set<String>> notesMap = usersNotes.computeIfAbsent(username, k -> new HashMap<>());
    Set<String> idsSet = notesMap.computeIfAbsent(noteType, k -> new LinkedHashSet<>());
    if (noteAction.equals("add") && noteIdIsCorrect(noteId)) {
      idsSet.remove(noteId);
      idsSet.add(noteId);
      while (noteType.equals("recent") && idsSet.size() > RECENT_NOTEBOOK_COUNT) {
        idsSet.remove(idsSet.iterator().next());
      }
    }
    if (noteAction.equals("remove")) {
      idsSet.remove(noteId);

      new Thread(() -> {
        try {
          Thread.sleep(1000);
          if (!noteIdIsCorrect(noteId)) {
            clearIncorrectNotesIds();
          }
        } catch (InterruptedException ignore) {
        }
      }).start();
    }

    return new JsonResponse(HttpStatus.OK, "is_" + noteType).build();
  }
}