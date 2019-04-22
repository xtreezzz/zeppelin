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

import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.nio.file.AccessDeniedException;
import java.time.LocalDateTime;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.exception.BadRequestException;
import org.apache.zeppelin.rest.exception.NoteNotFoundException;
import org.apache.zeppelin.rest.exception.ParagraphNotFoundException;
import org.apache.zeppelin.rest.message.NewNoteRequest;
import org.apache.zeppelin.rest.message.NewParagraphRequest;
import org.apache.zeppelin.rest.message.RenameNoteRequest;
import org.apache.zeppelin.rest.message.UpdateParagraphRequest;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.NoteInfo;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteService;
import ru.tinkoff.zeppelin.engine.search.LuceneSearch;

import java.io.IOException;
import java.util.*;

/**
 * Rest api endpoint for the zeppelinRepository.
 */
@RestController
@RequestMapping("/api/notebook")
public class NotebookRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(NotebookRestApi.class);
  private static final Gson gson = new Gson();

  private final LuceneSearch luceneSearch;
  private final ConnectionManager connectionManager;
  private final NoteService noteService;

  @Autowired
  public NotebookRestApi(
          final LuceneSearch luceneSearch,
          final ConnectionManager connectionManager,
          final NoteService noteRepository) {
    this.luceneSearch = luceneSearch;
    this.connectionManager = connectionManager;
    this.noteService = noteRepository;
  }

  /**
   * Get note authorization information.
   */
  @GetMapping(value = "/{noteId}/permissions", produces = "application/json")
  public ResponseEntity getNotePermissions(@PathVariable("noteId") final String noteId) throws IOException {
    checkIfUserCanRead(noteId,
            "Insufficient privileges you cannot get the list of permissions for this note");
    final HashMap<String, Set<String>> permissionsMap = new HashMap<>();
    Note target = noteService.getNote(noteId);
    if (target != null) {
      permissionsMap.put("owners", target.getOwners());
      permissionsMap.put("readers", target.getReaders());
      permissionsMap.put("writers", target.getWriters());
      permissionsMap.put("runners", target.getRunners());
    }
    return new JsonResponse(HttpStatus.OK, "", permissionsMap).build();
  }

  private String ownerPermissionError(final Set<String> current, final Set<String> allowed) {
    LOG.info("Cannot change permissions. Connection owners {}. Allowed owners {}",
            current.toString(), allowed.toString());
    return "Insufficient privileges to change permissions.\n\n" +
            "Allowed owners: " + allowed.toString() + "\n\n" +
            "User belongs to: " + current.toString();
  }

  private String getBlockNotAuthenticatedUserErrorMsg() {
    return "Only authenticated user can set the permission.";
  }

  /*
   * Set of utils method to check if current user can perform action to the note.
   * Since we only have security on zeppelinRepository level, from now we keep this logic in this class.
   * In the future we might want to generalize this for the rest of the api enmdpoints.
   */

  /**
   * Check if the current user own the given note.
   */
  private void checkIfUserIsOwner(final String noteId, final String errorMsg) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(authenticationInfo.getUser());
    userAndRoles.addAll(authenticationInfo.getRoles());
    //if (!notePermissionsService.isOwner(userAndRoles, noteId)) {
    //  throw new ForbiddenException(errorMsg);
    //}
  }

  /**
   * Check if the current user is either Owner or Writer for the given note.
   */
  private void checkIfUserCanWrite(final String noteId, final String errorMsg) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(authenticationInfo.getUser());
    userAndRoles.addAll(authenticationInfo.getRoles());
    //if (!notePermissionsService.hasWriteAuthorization(userAndRoles, noteId)) {
    //  throw new ForbiddenException(errorMsg);
    //}
  }

  /**
   * Check if the current user can access (at least he have to be reader) the given note.
   */
  private void checkIfUserCanRead(final String noteId, final String errorMsg) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(authenticationInfo.getUser());
    userAndRoles.addAll(authenticationInfo.getRoles());
    //if (!notePermissionsService.hasReadAuthorization(userAndRoles, noteId)) {
    //  throw new ForbiddenException(errorMsg);
    //}
  }

  /**
   * Check if the current user can run the given note.
   */
  private void checkIfUserCanRun(final String noteId, final String errorMsg) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(authenticationInfo.getUser());
    userAndRoles.addAll(authenticationInfo.getRoles());
    //if (!notePermissionsService.hasRunAuthorization(userAndRoles, noteId)) {
     // throw new ForbiddenException(errorMsg);
    //}
  }

  private void checkIfNoteIsNotNull(final Note note) {
    if (note == null) {
      throw new NoteNotFoundException("note not found");
    }
  }


  private void checkIfParagraphIsNotNull(final Paragraph paragraph) {
    if (paragraph == null) {
      throw new ParagraphNotFoundException("paragraph not found");
    }
  }

  private boolean userHasOwnerPermission(final Note note) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Set<String> admin = new HashSet<>();
    admin.addAll(Configuration.getAdminUsers());
    admin.addAll(Configuration.getAdminGroups());

    final Set<String> userRoles = new HashSet<>();
    userRoles.addAll(authenticationInfo.getRoles());
    userRoles.add(authenticationInfo.getUser());

    return userRoles.removeAll(admin) || userRoles.removeAll(note.getOwners());
  }

  private static String normalizePath(String path) {
    // fix 'folder/noteName' --> '/folder/noteName'
    if (!path.startsWith("/")) {
      path = "/" + path;
    }

    // fix '///folder//noteName' --> '/folder/noteName'
    while (path.contains("//")) {
      path = path.replaceAll("//", "/");
    }

    //fix '/folder/noteName/' --> '/folder/noteName'
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  private Paragraph getParagraph(final Note note, final String paragraphId) {
    return noteService.getParagraphs(note).stream()
        .filter(p -> p.getUuid().equals(paragraphId))
        .findAny()
        .orElseThrow(() -> new ParagraphNotFoundException("paragraph not found"));
  }

  /**
   * Set note authorization information.
   */
  @PutMapping(value = "/{noteId}/permissions", produces = "application/json")
  public ResponseEntity putNotePermissions(@PathVariable("noteId") final String noteId,
                                           final String req)
          throws IOException {

    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final HashSet<String> userAndRoles = new HashSet<>();
    userAndRoles.add(authenticationInfo.getUser());
    userAndRoles.addAll(authenticationInfo.getRoles());

    final HashMap<String, HashSet<String>> permMap =
            gson.fromJson(req, new TypeToken<HashMap<String, HashSet<String>>>() {
            }.getType());
    final Note note = noteService.getNote(noteId);

    checkIfUserIsOwner(noteId,
        ownerPermissionError(userAndRoles, note.getOwners()));

    LOG.info("Set permissions {} {} {} {} {} {}", noteId, authenticationInfo.getUser(), permMap.get("owners"),
            permMap.get("readers"), permMap.get("runners"), permMap.get("writers"));

    final HashSet<String> readers = permMap.get("readers");
    HashSet<String> runners = permMap.get("runners");
    HashSet<String> owners = permMap.get("owners");
    HashSet<String> writers = permMap.get("writers");
    // Set readers, if runners, writers and owners is empty -> set to user requesting the change
    if (readers != null && !readers.isEmpty()) {
      if (runners.isEmpty()) {
        runners = Sets.newHashSet(authenticationInfo.getUser());
      }
      if (writers.isEmpty()) {
        writers = Sets.newHashSet(authenticationInfo.getUser());
      }
      if (owners.isEmpty()) {
        owners = Sets.newHashSet(authenticationInfo.getUser());
      }
    }
    // Set runners, if writers and owners is empty -> set to user requesting the change
    if (runners != null && !runners.isEmpty()) {
      if (writers.isEmpty()) {
        writers = Sets.newHashSet(authenticationInfo.getUser());
      }
      if (owners.isEmpty()) {
        owners = Sets.newHashSet(authenticationInfo.getUser());
      }
    }
    // Set writers, if owners is empty -> set to user requesting the change
    if (writers != null && !writers.isEmpty()) {
      if (owners.isEmpty()) {
        owners = Sets.newHashSet(authenticationInfo.getUser());
      }
    }

    note.getReaders().clear();
    note.getReaders().addAll(readers);

    note.getRunners().clear();
    note.getRunners().addAll(runners);

    note.getWriters().clear();
    note.getWriters().addAll(writers);

    note.getOwners().clear();
    note.getOwners().addAll(owners);

    LOG.debug("After set permissions {} {} {} {}", note.getOwners(), note.getReaders(),
        note.getRunners(), note.getWriters());

    noteService.updateNote(note);
    //TODO(KOT): FIX
      /*
    zeppelinRepositoryServer.broadcastNote(note);
    zeppelinRepositoryServer.broadcastNoteList(subject, userAndRoles);
    */
    return new JsonResponse(HttpStatus.OK).build();
  }

  @GetMapping(produces = "application/json")
  public ResponseEntity getNoteList() {
    final List<NoteInfo> notesInfo = noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .map(NoteInfo::new)
        .collect(Collectors.toList());
    return new JsonResponse(HttpStatus.OK, "", notesInfo).build();
  }

  @GetMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity getNote(@PathVariable("noteId") final String noteId) {
    return new JsonResponse(HttpStatus.OK, "", noteService.getNote(noteId)).build();
  }

  /**
   * export note REST API.
   *
   * @param noteId ID of Note
   * @return note JSON with status.OK
   * @throws IOException
   */
  @GetMapping(value = "/export/{noteId}", produces = "application/json")
  public ResponseEntity exportNote(@PathVariable("noteId") final String noteId) throws IOException {
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot export this note");
    final String exportJson = null;//zeppelinRepository.exportNote(noteId);
    return new JsonResponse(HttpStatus.OK, "", exportJson).build();
  }

  /**
   * import new note REST API.
   *
   * @param noteJson - note Json
   * @return JSON with new note ID
   * @throws IOException
   */
  @PostMapping(value = "/import", produces = "application/json")
  public ResponseEntity importNote(final String noteJson) throws IOException {
    final Note note = null;//zeppelinRepository.importNote(null, noteJson, getServiceContext().getAutheInfo());
    return new JsonResponse(HttpStatus.OK, "", note.getUuid()).build();
  }

  /**
   * Create new note REST API.
   *
   * @param message - JSON with new note name
   * @return JSON with new note ID
   * @throws IOException
   */
  @PostMapping(produces = "application/json")
  public ResponseEntity createNote(final String message) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    LOG.info("Create new note by JSON {}", message);

    try {
      final NewNoteRequest request = NewNoteRequest.fromJson(message);
      final Note note = new Note(request.getName());
      note.getReaders().add(authenticationInfo.getUser());
      note.getRunners().add(authenticationInfo.getUser());
      note.getWriters().add(authenticationInfo.getUser());
      note.getOwners().add(authenticationInfo.getUser());
      noteService.persistNote(note);

      if (request.getParagraphs() != null) {
        for (final NewParagraphRequest paragraphRequest : request.getParagraphs()) {
          final Paragraph paragraph = new Paragraph();
          paragraph.setId(null);
          paragraph.setNoteId(note.getId());
          paragraph.setTitle(paragraphRequest.getTitle());
          paragraph.setText(paragraphRequest.getText());
          paragraph.setShebang(null);
          paragraph.setCreated(LocalDateTime.now());
          paragraph.setUpdated(LocalDateTime.now());
          paragraph.setPosition(paragraphRequest.getIndex());
          paragraph.setJobId(null);
          paragraph.setConfig(new HashMap<>());
          paragraph.setFormParams(new HashMap<>());
          noteService.persistParagraphSilently(paragraph);
        }
      }
      return new JsonResponse(HttpStatus.OK, "", note.getUuid()).build();
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to create note.", e);
    }
  }


  /**
   * Delete note REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.OK
   */
  @DeleteMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity deleteNote(@PathVariable("noteId") final String noteId) throws IOException {
    LOG.info("Delete note {} ", noteId);
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = noteService.getNote(noteId);
    if (!userHasOwnerPermission(note)) {
      throw new AccessDeniedException(
          "User " + authenticationInfo.getUser() +
              " is no owner note " + note.getId()
      );
    }
    noteService.deleteNote(note);
    return new JsonResponse(HttpStatus.OK, "").build();
  }


  /**
   * Clone note REST API.
   *
   * @param noteId ID of Note
   */
  @PostMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity cloneNote(@PathVariable("noteId") final String noteId, final String message)
          throws IOException, IllegalArgumentException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    LOG.info("clone note by JSON {}", message);
    checkIfUserCanWrite(noteId, "Insufficient privileges you cannot clone this note");
    Note note = noteService.getNote(noteId);
    final NewNoteRequest request = NewNoteRequest.fromJson(message);
    String path = normalizePath(request.getName());

    Note cloneNote = new Note(path);
    cloneNote.setPath(path);
    cloneNote.setScheduler(note.getScheduler());
    cloneNote.getReaders().clear();
    cloneNote.getRunners().clear();
    cloneNote.getWriters().clear();
    cloneNote.getOwners().clear();
    cloneNote.getReaders().add(authenticationInfo.getUser());
    cloneNote.getRunners().add(authenticationInfo.getUser());
    cloneNote.getWriters().add(authenticationInfo.getUser());
    cloneNote.getOwners().add(authenticationInfo.getUser());
    cloneNote = noteService.persistNote(cloneNote);

    final List<Paragraph> paragraphs = noteService.getParagraphs(note);
    for (final Paragraph paragraph : paragraphs) {
      final Paragraph cloneParagraph = new Paragraph();
      cloneParagraph.setId(null);
      cloneParagraph.setNoteId(cloneNote.getId());
      cloneParagraph.setTitle(paragraph.getTitle());
      cloneParagraph.setText(paragraph.getText());
      cloneParagraph.setShebang(paragraph.getShebang());
      cloneParagraph.setCreated(LocalDateTime.now());
      cloneParagraph.setUpdated(LocalDateTime.now());
      cloneParagraph.setPosition(paragraph.getPosition());
      cloneParagraph.setJobId(null);
      cloneParagraph.setConfig(paragraph.getConfig());
      cloneParagraph.setFormParams(paragraph.getFormParams());
      noteService.persistParagraphSilently(cloneParagraph);
    }

    return new JsonResponse(HttpStatus.OK, "", cloneNote.getUuid()).build();
  }

  /**
   * Rename note REST API
   *
   * @param message - JSON containing new name
   * @return JSON with status.OK
   */
  @PutMapping(value = "/{noteId}/rename", produces = "application/json")
  public ResponseEntity renameNote(@PathVariable("noteId") final String noteId,
                                   final String message) throws IOException {
    LOG.info("rename note by JSON {}", message);
    final RenameNoteRequest request = gson.fromJson(message, RenameNoteRequest.class);
    final String path = request.getPath();
    if (path.isEmpty()) {
      LOG.warn("Trying to rename zeppelinRepository {} with empty path parameter", noteId);
      throw new BadRequestException("path can not be empty");
    }
    final Note note = noteService.getNote(noteId);
    note.setPath(path);
    noteService.updateNote(note);
    return new JsonResponse(HttpStatus.OK, "").build();
  }

  /**
   * Insert paragraph REST API.
   *
   * @param message - JSON containing paragraph's information
   */
  @PostMapping(value = "/{noteId}/paragraph", produces = "application/json")
  public ResponseEntity insertParagraph(@PathVariable("noteId") final String noteId, final String message)
          throws IOException {

    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    LOG.info("insert paragraph {} {}", noteId, message);

    final Note note = noteService.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanWrite(noteId, "Insufficient privileges you cannot add paragraph to this note");

    final NewParagraphRequest request = NewParagraphRequest.fromJson(message);
    final Integer index = request.getIndex();
    final List<Paragraph> paragraphs = noteService.getParagraphs(note);

    for (int i = index; i < paragraphs.size(); i++) {
      final Paragraph p = paragraphs.get(i);
      p.setPosition(i + 1);
      noteService.updateParagraph(note, p);
    }

    Paragraph paragraph = new Paragraph();
    paragraph.setId(null);
    paragraph.setNoteId(note.getId());
    paragraph.setTitle(StringUtils.EMPTY);
    paragraph.setText(request.getText());
    paragraph.setShebang(null);
    paragraph.setCreated(LocalDateTime.now());
    paragraph.setUpdated(LocalDateTime.now());
    paragraph.setPosition(index);
    paragraph.setJobId(null);
    paragraph.setConfig(new HashMap<>());
    paragraph.setFormParams(new HashMap<>());
    paragraph = noteService.persistParagraph(note, paragraph);

    return new JsonResponse(HttpStatus.OK, "", paragraph.getUuid()).build();
  }


  /**
   * Get paragraph REST API.
   *
   * @param noteId ID of Note
   * @return JSON with information of the paragraph
   */
  @GetMapping(value = "/{noteId}/paragraph/{paragraphId}", produces = "application/json")
  public ResponseEntity getParagraph(
      @PathVariable("noteId") final String noteId,
      @PathVariable("paragraphId") final String paragraphId) {
    LOG.info("get paragraph {} {}", noteId, paragraphId);

    Note note = noteService.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get this paragraph");

    return new JsonResponse(HttpStatus.OK, "", getParagraph(note, paragraphId)).build();
  }

  /**
   * Update paragraph.
   *
   * @param message json containing the "text" and optionally the "title" of the paragraph, e.g.
   *                {"text" : "updated text", "title" : "Updated title" }
   */
  @PutMapping(value = "/{noteId}/paragraph/{paragraphId}", produces = "application/json")
  public ResponseEntity updateParagraph(@PathVariable("noteId") final String noteId,
                                        @PathVariable("paragraphId") final String paragraphId,
                                        final String message) throws IOException {

    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    LOG.info("{} will update paragraph {} {}", authenticationInfo.getUser(), noteId, paragraphId);

    final Note note = noteService.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanWrite(noteId, "Insufficient privileges you cannot update this paragraph");
    final Paragraph paragraph = getParagraph(note, paragraphId);

    final UpdateParagraphRequest updatedParagraph = gson.fromJson(message, UpdateParagraphRequest.class);
    paragraph.setText(updatedParagraph.getText());

    if (updatedParagraph.getTitle() != null) {
      paragraph.setTitle(updatedParagraph.getTitle());
    }

    noteService.updateParagraph(note, paragraph);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.PARAGRAPH).put("paragraph", paragraph));
    return new JsonResponse(HttpStatus.OK, "").build();
  }


  @PutMapping(value = "/{noteId}/paragraph/{paragraphId}/config", produces = "application/json")
  public ResponseEntity updateParagraphConfig(@PathVariable("noteId") final String noteId,
                                              @PathVariable("paragraphId") final String paragraphId,
                                              final String message) throws IOException {

    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    LOG.info("{} will update paragraph config {} {}", authenticationInfo.getUser(), noteId, paragraphId);

    final Note note = noteService.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanWrite(noteId, "Insufficient privileges you cannot update this paragraph config");
    final Paragraph paragraph = getParagraph(note, paragraphId);
    checkIfParagraphIsNotNull(paragraph);

    final Map<String, Object> newConfig = gson.fromJson(message, HashMap.class);
    paragraph.setConfig(newConfig);
    noteService.updateParagraph(note, paragraph);
    return new JsonResponse(HttpStatus.OK, "").build();
  }

  /**
   * Move paragraph REST API.
   */
  @PostMapping(value = "/{noteId}/paragraph/{paragraphId}/move/{newIndex}", produces = "application/json")
  public ResponseEntity moveParagraph(@PathVariable("noteId") final String noteId,
                                @PathVariable("paragraphId") final String paragraphId,
                                @PathVariable("newIndex") final int index)
          throws IOException {
    LOG.info("move paragraph {} {} {}", noteId, paragraphId, index);

    final Note note = noteService.getNote(noteId);
//    if (index > 0 && index >= note.getParagraphs().size()) {
//      throw new BadRequestException("newIndex " + index + " is out of bounds");
//    }

    //note.moveParagraph(paragraphId, index);
    noteService.updateNote(note);

    final SockMessage message = new SockMessage(Operation.PARAGRAPH_MOVED)
            .put("id", paragraphId)
            .put("index", index);
    connectionManager.broadcast(note.getId(), message);

    return new JsonResponse(HttpStatus.OK, "").build();

  }


  /**
   * Delete paragraph REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.OK
   * @throws IOException
   */
  @DeleteMapping(value = "/{noteId}/paragraph/{paragraphId}", produces = "application/json")
  public ResponseEntity deleteParagraph(@PathVariable("noteId") final String noteId,
                                  @PathVariable("paragraphId") final String paragraphId) throws IOException {
    LOG.info("delete paragraph {} {}", noteId, paragraphId);

    final Note note = noteService.getNote(noteId);

    //note.getParagraphs().removeIf(paragraph -> paragraph.getId().equals(paragraphId));
    //note.removeParagraph(getServiceContext().getAutheInfo().getUser(), paragraphId);

    noteService.updateNote(note);

    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));

    return new JsonResponse(HttpStatus.OK, "").build();
  }

  /**
   * Clear result of all paragraphs REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.ok
   */
  @PutMapping(value = "/{noteId}/clear", produces = "application/json")
  public ResponseEntity clearAllParagraphOutput(@PathVariable("noteId") final String noteId)
          throws IOException {
    LOG.info("clear all paragraph output of note {}", noteId);

    final Note note = noteService.getNote(noteId);

    //note.clearAllParagraphOutput();

    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));

    return new JsonResponse(HttpStatus.OK, "").build();
  }

  /**
   * Run note jobs REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  @PostMapping(value = "/job/{noteId}", produces = "application/json")
  public ResponseEntity runNoteJobs(@PathVariable("noteId") final String noteId,
                                    @RequestParam("waitToFinish") final Boolean waitToFinish)
          throws IOException, IllegalArgumentException {
    final boolean blocking = waitToFinish == null || waitToFinish;
    LOG.info("run note jobs {} waitToFinish: {}", noteId, blocking);
    final Note note = noteService.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRun(noteId, "Insufficient privileges you cannot run job for this note");

    try {
      //TODO(egorklimov): Исполнение было убрано из Note
      //note.runAllParagraphs(subject, blocking);
    } catch (final Exception ex) {
      LOG.error("Exception from run", ex);
      return new JsonResponse(HttpStatus.PRECONDITION_FAILED,
              ex.getMessage() + "- Not selected or Invalid Interpreter bind").build();
    }
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Stop(delete) note jobs REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  @DeleteMapping(value = "/job/{noteId}", produces = "application/json")
  public ResponseEntity stopNoteJobs(@PathVariable("noteId") final String noteId) throws IllegalArgumentException {
    LOG.info("stop note jobs {} ", noteId);
    final Note note = noteService.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRun(noteId, "Insufficient privileges you cannot stop this job for this note");

    //TODO(egorklimov): информация о выполнении была убрана из парграфа
    //    for (final Paragraph p : note.getParagraphs()) {
    //      if (!p.isTerminated()) {
    //        p.abort();
    //      }
    //    }
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Get note job status REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  @GetMapping(value = "/job/{noteId}", produces = "application/json")
  public ResponseEntity getNoteJobStatus(@PathVariable("noteId") final String noteId)
          throws IOException, IllegalArgumentException {
    LOG.info("get note job status.");
    final Note note = noteService.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get job status");

    return new JsonResponse(HttpStatus.OK, null, null).build();
    //return new JsonResponse(HttpStatus.OK, null, note.generateParagraphsInfo()).build();
  }

  /**
   * Get note paragraph job status REST API.
   *
   * @param noteId      ID of Note
   * @param paragraphId ID of Paragraph
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  @GetMapping(value = "/job/{noteId}/{paragraphId}", produces = "application/json")
  public ResponseEntity getNoteParagraphStatus(@PathVariable("noteId") final String noteId,
                                                  @PathVariable("paragraphId") final String paragraphId)
          throws IllegalArgumentException {
    LOG.info("get note paragraph job status.");
    final Note note = noteService.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get job status");

    final Paragraph paragraph = null;//note.getParagraph(paragraphId);
    checkIfParagraphIsNotNull(paragraph);

    return new JsonResponse(HttpStatus.OK, null, null).build();
    //return new JsonResponse(HttpStatus.OK, null, note.generateSingleParagraphInfo(paragraphId)).build();
  }

  /**
   * Run asynchronously paragraph job REST API.
   *
   * @param message - JSON with params if user wants to update dynamic form's value
   *                null, empty string, empty json if user doesn't want to update
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  //TODO(KOT): FIX
      /*
  //@POST
  //@Path("job/{noteId}/{paragraphId}")
  @ZeppelinApi
  @PostMapping(value = "/job/{noteId}/{paragraphId}", produces = "application/json")
  public ResponseEntity runParagraph(@PathVariable("noteId") String noteId,
                               @PathVariable("paragraphId") String paragraphId, String message)
          throws IOException, IllegalArgumentException {
    LOG.info("run paragraph job asynchronously {} {} {}", noteId, paragraphId, message);

    Note note = zeppelinRepository.getNote(noteId);
    checkIfNoteIsNotNull(note);
    Paragraph paragraph = note.getParagraph(paragraphId);
    checkIfParagraphIsNotNull(paragraph);

    Map<String, Object> params = new HashMap<>();
    if (!StringUtils.isEmpty(message)) {
      RunParagraphWithParametersRequest request =
              RunParagraphWithParametersRequest.fromJson(message);
      params = request.getParams();
    }
    zeppelinRepositoryService.runParagraph(noteId, paragraphId, paragraph.getTitle(),
            paragraph.getText(), null, params, new HashMap<>(),
            false, false, getServiceContext(), new RestServiceCallback<>());
    return new JsonResponse(HttpStatus.OK).build();
  }
  */

  /**
   * Run synchronously a paragraph REST API.
   *
   * @param noteId      - noteId
   * @param paragraphId - paragraphId
   * @param message     - JSON with params if user wants to update dynamic form's value
   *                    null, empty string, empty json if user doesn't want to update
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  //TODO(KOT): FIX
      /*
  //@POST
  //@Path("run/{noteId}/{paragraphId}")
  @ZeppelinApi
  @PostMapping(value = "/run/{noteId}/{paragraphId}", produces = "application/json")
  public ResponseEntity runParagraphSynchronously(@PathVariable("noteId") String noteId,
                                            @PathVariable("paragraphId") String paragraphId,
                                            String message)
          throws IOException, IllegalArgumentException {
    LOG.info("run paragraph synchronously {} {} {}", noteId, paragraphId, message);

    Note note = zeppelinRepository.getNote(noteId);
    checkIfNoteIsNotNull(note);
    Paragraph paragraph = note.getParagraph(paragraphId);
    checkIfParagraphIsNotNull(paragraph);

    Map<String, Object> params = new HashMap<>();
    if (!StringUtils.isEmpty(message)) {
      RunParagraphWithParametersRequest request =
              RunParagraphWithParametersRequest.fromJson(message);
      params = request.getParams();
    }

    if (zeppelinRepositoryService.runParagraph(noteId, paragraphId, paragraph.getTitle(),
            paragraph.getText(), params,
            new HashMap<>(), false, true, getServiceContext(), new RestServiceCallback<>())) {
      note = zeppelinRepositoryService.getNote(noteId, getServiceContext(), new RestServiceCallback<>());
      Paragraph p = note.getParagraph(paragraphId);
      InterpreterResult result = p.getReturn();
      if (result.code() == InterpreterResult.Code.SUCCESS) {
        return new JsonResponse(HttpStatus.OK, result).build();
      } else {
        return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, result).build();
      }
    } else {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Fail to run paragraph").build();
    }
  }
*/

  /**
   * Stop(delete) paragraph job REST API.
   *
   * @param noteId      ID of Note
   * @param paragraphId ID of Paragraph
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  //TODO(KOT): FIX
      /*
  //@DELETE
  //@Path("job/{noteId}/{paragraphId}")
  @ZeppelinApi
  @DeleteMapping(value = "job/{noteId}/{paragraphId}", produces = "application/json")
  public ResponseEntity cancelParagraph(@PathVariable("noteId") String noteId,
                                  @PathVariable("paragraphId") String paragraphId)
          throws IOException, IllegalArgumentException {
    LOG.info("stop paragraph job {} ", noteId);
    zeppelinRepositoryService.cancelParagraph(noteId, paragraphId, getServiceContext(),
            new RestServiceCallback<Paragraph>());
    return new JsonResponse(HttpStatus.OK).build();
  }
  */

  /**
   * Set paragraph form value
   *
   * @return JSON with status.OK
   */
  @PostMapping(value = "/{noteId}/{paragraphId}/form_values", produces = "application/json")
  public ResponseEntity setFormValue(
      @PathVariable("noteId") String noteIdParam,
      @PathVariable("paragraphId") String paragraphIdIdParam,
      @RequestBody Map<String, Object> formValues
  ) {
    long noteId = Long.valueOf(noteIdParam);
    long paragraphId = Long.valueOf(paragraphIdIdParam);
//    checkIfUserCanWrite(noteId, "Insufficient privileges you cannot change form values");
    Note note = noteService.getNote(noteId);
    noteService.getParagraphs(note)
        .stream()
        .filter(p -> p.getId() == paragraphId)
        .findAny()
        .ifPresent(p -> {
          Map<String, Object> params = p.getFormParams();
          params.clear();
          params.putAll(formValues);
          noteService.updateParagraph(note, p);
        });
    return new JsonResponse(HttpStatus.OK).build();
  }


  /**
   * Get note jobs for job manager.
   *
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  //TODO(KOT): FIX
  @GetMapping(value = "/jobmanager/", produces = "application/json")
  public ResponseEntity getJobListforNote() throws IOException, IllegalArgumentException {
    LOG.info("Get note jobs for job manager");
    //final List<JobManagerService.NoteJobInfo> noteJobs = jobManagerService.getNoteJobInfoByUnixTime(0);
    final Map<String, Object> response = new HashMap<>();
    response.put("lastResponseUnixTime", System.currentTimeMillis());
    response.put("jobs", null);
    return new JsonResponse(HttpStatus.OK, response).build();
  }

  /**
   * Get updated note jobs for job manager
   * <p>
   * Return the `Note` change information within the post unix timestamp.
   *
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  @GetMapping(value = "/jobmanager/{lastUpdateUnixtime}/", produces = "application/json")
  public ResponseEntity getUpdatedJobListforNote(@PathVariable("lastUpdateUnixtime") long lastUpdateUnixTime)
          throws IOException, IllegalArgumentException {
    LOG.info("Get updated note jobs lastUpdateTime {}", lastUpdateUnixTime);
    //List<JobManagerService.NoteJobInfo> noteJobs = jobManagerService.getNoteJobInfoByUnixTime(lastUpdateUnixTime);
    Map<String, Object> response = new HashMap<>();
    response.put("lastResponseUnixTime", System.currentTimeMillis());
    response.put("jobs", null);
    return new JsonResponse(HttpStatus.OK, response).build();
  }

  /**
   * Search for a Notes with permissions.
   */
  @GetMapping(value = "/search", produces = "application/json")
  public ResponseEntity search(@RequestParam("q") final String queryTerm) {
    LOG.info("Searching notes for: {}", queryTerm);

    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final HashSet<String> userAndRoles = new HashSet<>();
    userAndRoles.add(authenticationInfo.getUser());
    userAndRoles.addAll(authenticationInfo.getRoles());

    final List<Map<String, String>> notesFound = luceneSearch.query(queryTerm);
    for (int i = 0; i < notesFound.size(); i++) {
      final String[] ids = notesFound.get(i).get("id").split("/", 2);
      final String noteId = ids[0];
      //if (!notePermissionsService.isOwner(noteId, userAndRoles) &&
      //        !notePermissionsService.isReader(noteId, userAndRoles) &&
      //        !notePermissionsService.isWriter(noteId, userAndRoles) &&
      //        !notePermissionsService.isRunner(noteId, userAndRoles)) {
      //  notesFound.remove(i);
      //  i--;
      //}
    }
    return new JsonResponse(HttpStatus.OK, notesFound).build();
  }
}