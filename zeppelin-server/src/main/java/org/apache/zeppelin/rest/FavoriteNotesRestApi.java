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
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.apache.shiro.SecurityUtils;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/favorite_notes")
@Produces("application/json")
@Singleton
public class FavoriteNotesRestApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(FavoriteNotesRestApi.class);

  private static final Integer RECENT_NOTEBOOK_COUNT = 7;
  private static final Integer SAVE_ON_DISK_INTERVAL = 5; // minutes
  private static volatile Boolean saveTaskCreate = false;

  private static File dataFile;
  private static Notebook notebook;
  private static Map<String, Map<String, Set<String>>> usersNotes;
  private static ScheduledExecutorService saveExecServ = Executors.newSingleThreadScheduledExecutor();

  @Inject
  public FavoriteNotesRestApi(Notebook notebook) {
    String filePath = ZeppelinConfiguration.create().getFavoriteNotesFilePath();
    dataFile = new File(filePath);
    usersNotes = loadNotesList();
    FavoriteNotesRestApi.notebook = notebook;
    clearIncorrectNotesIds();

    synchronized (saveTaskCreate) {
      if (!saveTaskCreate) {
        Runtime.getRuntime().addShutdownHook(new Thread(this::saveNotesList));
        saveExecServ.scheduleWithFixedDelay(this::saveNotesList, SAVE_ON_DISK_INTERVAL, SAVE_ON_DISK_INTERVAL, TimeUnit.MINUTES);
        saveTaskCreate = true;
      }
    }
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
    Note note = notebook.getNote(noteId);
    if (note == null) {
      return false;
    }
    return !note.isTrash();
  }

  private void clearIncorrectNotesIds() {
    for (Map<String, Set<String>> userList : usersNotes.values()) {
      for (Set<String> noteType : userList.values()) {
        noteType.removeIf(id -> !noteIdIsCorrect(id));
      }
    }
  }

  @GET
  @Path("get_notes_ids")
  @ZeppelinApi
  public Response getNotesIds(@QueryParam("username") String username) {
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
      return new JsonResponse<>(Response.Status.OK, "", idsMap).build();
    } catch (NullPointerException ignore) {
      return new JsonResponse<>(Response.Status.OK, "", Collections.emptyList()).build();
    }
  }

  @GET
  @Path("set_note_status")
  @ZeppelinApi
  public Response setNoteStatus(@QueryParam("username") String username,
                                @QueryParam("note_id") String noteId,
                                @QueryParam("note_type") String noteType,
                                @QueryParam("note_action") String noteAction) {

    if (username == null || username.isEmpty() || !SecurityUtils.getSubject().getPrincipal().equals(username)) {
      return new JsonResponse<>(Response.Status.BAD_REQUEST, "username no correct", null).build();
    }
    if (noteId == null || noteId.isEmpty()) {
      return new JsonResponse<>(Response.Status.BAD_REQUEST, "note_id no correct", null).build();
    }
    if (noteType == null || noteType.isEmpty() || !(noteType.equals("favorite") || noteType.equals("recent"))) {
      return new JsonResponse<>(Response.Status.BAD_REQUEST, "note_type no correct", null).build();
    }
    if (noteAction == null || noteAction.isEmpty() || !(noteAction.equals("add") || noteAction.equals("remove"))) {
      return new JsonResponse<>(Response.Status.BAD_REQUEST, "note_action no correct", null).build();
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

    return new JsonResponse<>(Response.Status.OK, "is_" + noteType, null).build();
  }
}