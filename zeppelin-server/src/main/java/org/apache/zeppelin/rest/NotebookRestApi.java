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
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.*;
import org.apache.zeppelin.rest.exception.BadRequestException;
import org.apache.zeppelin.rest.exception.ForbiddenException;
import org.apache.zeppelin.rest.exception.NoteNotFoundException;
import org.apache.zeppelin.rest.exception.ParagraphNotFoundException;
import org.apache.zeppelin.rest.message.*;
import org.apache.zeppelin.search.SearchService;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.JobManagerService;
import org.apache.zeppelin.service.SecurityService;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;

/**
 * Rest api endpoint for the notebook.
 */
@RestController
@RequestMapping("/api/notebook")
public class NotebookRestApi extends AbstractRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(NotebookRestApi.class);
  private static final Gson gson = new Gson();

  private final ZeppelinConfiguration zConf;
  private final Notebook notebook;
  private final SearchService noteSearchService;
  private final NotebookAuthorization notebookAuthorization;
  private final JobManagerService jobManagerService;
  private final SecurityService securityService;
  private final ConnectionManager connectionManager;

  @Autowired
  public NotebookRestApi(
          final Notebook notebook,
          final SearchService search,
          final NotebookAuthorization notebookAuthorization,
          final ZeppelinConfiguration zConf,
          @Qualifier("NoSecurityService") final SecurityService securityService,
          final JobManagerService jobManagerService,
          final ConnectionManager connectionManager) {
    super(securityService);
    this.notebook = notebook;
    this.jobManagerService = jobManagerService;
    this.noteSearchService = search;
    this.notebookAuthorization = notebookAuthorization;
    this.zConf = zConf;
    this.securityService = securityService;
    this.connectionManager = connectionManager;
  }

  /**
   * Get note authorization information.
   */
  @ZeppelinApi
  @GetMapping(value = "/{noteId}/permissions", produces = "application/json")
  public ResponseEntity getNotePermissions(@PathVariable("noteId") final String noteId) throws IOException {
    checkIfUserIsAnon(getBlockNotAuthenticatedUserErrorMsg());
    checkIfUserCanRead(noteId,
            "Insufficient privileges you cannot get the list of permissions for this note");
    final HashMap<String, Set<String>> permissionsMap = new HashMap<>();
    permissionsMap.put("owners", notebookAuthorization.getOwners(noteId));
    permissionsMap.put("readers", notebookAuthorization.getReaders(noteId));
    permissionsMap.put("writers", notebookAuthorization.getWriters(noteId));
    permissionsMap.put("runners", notebookAuthorization.getRunners(noteId));
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
   * Since we only have security on notebook level, from now we keep this logic in this class.
   * In the future we might want to generalize this for the rest of the api enmdpoints.
   */

  /**
   * Check if the current user is not authenticated(anonymous user) or not.
   */
  private void checkIfUserIsAnon(final String errorMsg) {
    final boolean isAuthenticated = securityService.isAuthenticated();
    if (isAuthenticated && securityService.getPrincipal().equals("anonymous")) {
      LOG.info("Anonymous user cannot set any permissions for this note.");
      throw new ForbiddenException(errorMsg);
    }
  }

  /**
   * Check if the current user own the given note.
   */
  private void checkIfUserIsOwner(final String noteId, final String errorMsg) {
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(securityService.getPrincipal());
    userAndRoles.addAll(securityService.getAssociatedRoles());
    if (!notebookAuthorization.isOwner(userAndRoles, noteId)) {
      throw new ForbiddenException(errorMsg);
    }
  }

  /**
   * Check if the current user is either Owner or Writer for the given note.
   */
  private void checkIfUserCanWrite(final String noteId, final String errorMsg) {
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(securityService.getPrincipal());
    userAndRoles.addAll(securityService.getAssociatedRoles());
    if (!notebookAuthorization.hasWriteAuthorization(userAndRoles, noteId)) {
      throw new ForbiddenException(errorMsg);
    }
  }

  /**
   * Check if the current user can access (at least he have to be reader) the given note.
   */
  private void checkIfUserCanRead(final String noteId, final String errorMsg) {
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(securityService.getPrincipal());
    userAndRoles.addAll(securityService.getAssociatedRoles());
    if (!notebookAuthorization.hasReadAuthorization(userAndRoles, noteId)) {
      throw new ForbiddenException(errorMsg);
    }
  }

  /**
   * Check if the current user can run the given note.
   */
  private void checkIfUserCanRun(final String noteId, final String errorMsg) {
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(securityService.getPrincipal());
    userAndRoles.addAll(securityService.getAssociatedRoles());
    if (!notebookAuthorization.hasRunAuthorization(userAndRoles, noteId)) {
      throw new ForbiddenException(errorMsg);
    }
  }

  private void checkIfNoteIsNotNull(final Note note) {
    if (note == null) {
      throw new NoteNotFoundException("note not found");
    }
  }

  private void checkIfNoteSupportsCron(final Note note) {
    if (!note.isCronSupported(notebook.getConf())) {
      LOG.error("Cron is not enabled from Zeppelin server");
      throw new ForbiddenException("Cron is not enabled from Zeppelin server");
    }
  }

  private void checkIfParagraphIsNotNull(final Paragraph paragraph) {
    if (paragraph == null) {
      throw new ParagraphNotFoundException("paragraph not found");
    }
  }

  /**
   * Set note authorization information.
   */
  @ZeppelinApi
  @PutMapping(value = "/{noteId}/permissions", produces = "application/json")
  public ResponseEntity putNotePermissions(@PathVariable("noteId") final String noteId,
                                           final String req)
          throws IOException {
    final String principal = securityService.getPrincipal();
    final Set<String> roles = securityService.getAssociatedRoles();
    final HashSet<String> userAndRoles = new HashSet<>();
    userAndRoles.add(principal);
    userAndRoles.addAll(roles);

    checkIfUserIsAnon(getBlockNotAuthenticatedUserErrorMsg());
    checkIfUserIsOwner(noteId,
            ownerPermissionError(userAndRoles, notebookAuthorization.getOwners(noteId)));

    final HashMap<String, HashSet<String>> permMap =
            gson.fromJson(req, new TypeToken<HashMap<String, HashSet<String>>>() {
            }.getType());
    final Note note = notebook.getNote(noteId);

    LOG.info("Set permissions {} {} {} {} {} {}", noteId, principal, permMap.get("owners"),
            permMap.get("readers"), permMap.get("runners"), permMap.get("writers"));

    final HashSet<String> readers = permMap.get("readers");
    HashSet<String> runners = permMap.get("runners");
    HashSet<String> owners = permMap.get("owners");
    HashSet<String> writers = permMap.get("writers");
    // Set readers, if runners, writers and owners is empty -> set to user requesting the change
    if (readers != null && !readers.isEmpty()) {
      if (runners.isEmpty()) {
        runners = Sets.newHashSet(securityService.getPrincipal());
      }
      if (writers.isEmpty()) {
        writers = Sets.newHashSet(securityService.getPrincipal());
      }
      if (owners.isEmpty()) {
        owners = Sets.newHashSet(securityService.getPrincipal());
      }
    }
    // Set runners, if writers and owners is empty -> set to user requesting the change
    if (runners != null && !runners.isEmpty()) {
      if (writers.isEmpty()) {
        writers = Sets.newHashSet(securityService.getPrincipal());
      }
      if (owners.isEmpty()) {
        owners = Sets.newHashSet(securityService.getPrincipal());
      }
    }
    // Set writers, if owners is empty -> set to user requesting the change
    if (writers != null && !writers.isEmpty()) {
      if (owners.isEmpty()) {
        owners = Sets.newHashSet(securityService.getPrincipal());
      }
    }

    notebookAuthorization.setReaders(noteId, readers);
    notebookAuthorization.setRunners(noteId, runners);
    notebookAuthorization.setWriters(noteId, writers);
    notebookAuthorization.setOwners(noteId, owners);
    LOG.debug("After set permissions {} {} {} {}", notebookAuthorization.getOwners(noteId),
            notebookAuthorization.getReaders(noteId), notebookAuthorization.getRunners(noteId),
            notebookAuthorization.getWriters(noteId));
    notebook.saveNote(note);
    //TODO(KOT): FIX
      /*
    notebookServer.broadcastNote(note);
    notebookServer.broadcastNoteList(subject, userAndRoles);
    */
    return new JsonResponse(HttpStatus.OK).build();
  }

  @ZeppelinApi
  @GetMapping(produces = "application/json")
  public ResponseEntity getNoteList() {
    final List<NoteInfo> notesInfo = notebook.getNotesInfo(getServiceContext().getUserAndRoles());
    return new JsonResponse(HttpStatus.OK, "", notesInfo).build();
  }

  @ZeppelinApi
  @GetMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity getNote(@PathVariable("noteId") final String noteId) {
    return new JsonResponse(HttpStatus.OK, "", notebook.getNote(noteId)).build();
  }

  /**
   * export note REST API.
   *
   * @param noteId ID of Note
   * @return note JSON with status.OK
   * @throws IOException
   */
  @ZeppelinApi
  @GetMapping(value = "/export/{noteId}", produces = "application/json")
  public ResponseEntity exportNote(@PathVariable("noteId") final String noteId) throws IOException {
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot export this note");
    final String exportJson = notebook.exportNote(noteId);
    return new JsonResponse(HttpStatus.OK, "", exportJson).build();
  }

  /**
   * import new note REST API.
   *
   * @param noteJson - note Json
   * @return JSON with new note ID
   * @throws IOException
   */
  @ZeppelinApi
  @PostMapping(value = "/import", produces = "application/json")
  public ResponseEntity importNote(final String noteJson) throws IOException {
    final Note note = notebook.importNote(null, noteJson, getServiceContext().getAutheInfo());
    return new JsonResponse(HttpStatus.OK, "", note.getId()).build();
  }

  /**
   * Create new note REST API.
   *
   * @param message - JSON with new note name
   * @return JSON with new note ID
   * @throws IOException
   */
  @ZeppelinApi
  @PostMapping(produces = "application/json")
  public ResponseEntity createNote(final String message) throws IOException {
    final String user = securityService.getPrincipal();
    LOG.info("Create new note by JSON {}", message);
    final NewNoteRequest request = NewNoteRequest.fromJson(message);
    final Note note = notebook.createNote(
        request.getName(),
        zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT),
        getServiceContext().getAutheInfo());
    final AuthenticationInfo subject = new AuthenticationInfo(securityService.getPrincipal());
    if (request.getParagraphs() != null) {
      for (final NewParagraphRequest paragraphRequest : request.getParagraphs()) {
        final Paragraph p = note.addNewParagraph(subject);
        initParagraph(p, paragraphRequest, user);
      }
    }
    return new JsonResponse(HttpStatus.OK, "", note.getId()).build();
  }


  /**
   * Delete note REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.OK
   */
  @ZeppelinApi
  @DeleteMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity deleteNote(@PathVariable("noteId") final String noteId) throws IOException {
    LOG.info("Delete note {} ", noteId);
    notebook.removeNote(noteId, getServiceContext().getAutheInfo());

    final List<NoteInfo> notesInfo = notebook.getNotesInfo(getServiceContext().getUserAndRoles());
    connectionManager.broadcast(new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo));

    return new JsonResponse(HttpStatus.OK, "").build();
  }


  /**
   * Clone note REST API.
   *
   * @param noteId ID of Note
   */
  @ZeppelinApi
  @PostMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity cloneNote(@PathVariable("noteId") final String noteId, final String message)
          throws IOException, IllegalArgumentException {
    LOG.info("clone note by JSON {}", message);
    checkIfUserCanWrite(noteId, "Insufficient privileges you cannot clone this note");
    final NewNoteRequest request = NewNoteRequest.fromJson(message);
    String newNoteName = null;
    if (request != null) {
      newNoteName = request.getName();
    }
    final Note newNote = notebook.cloneNote(noteId, newNoteName, getServiceContext().getAutheInfo());

    connectionManager.broadcast(newNote.getId(), new SockMessage(Operation.NOTE).put("note", newNote));

    final List<NoteInfo> notesInfo = notebook.getNotesInfo(getServiceContext().getUserAndRoles());
    connectionManager.broadcast( new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo));

    return new JsonResponse(HttpStatus.OK, "", newNote.getId()).build();
  }

  /**
   * Rename note REST API
   *
   * @param message - JSON containing new name
   * @return JSON with status.OK
   */
  @ZeppelinApi
  @PutMapping(value = "/{noteId}/rename", produces = "application/json")
  public ResponseEntity renameNote(@PathVariable("noteId") final String noteId,
                                   final String message) throws IOException {
    LOG.info("rename note by JSON {}", message);
    final RenameNoteRequest request = gson.fromJson(message, RenameNoteRequest.class);
    final String name = request.getName();
    if (name.isEmpty()) {
      LOG.warn("Trying to rename notebook {} with empty name parameter", noteId);
      throw new BadRequestException("name can not be empty");
    }
    final Note note = notebook.getNote(noteId);
    String newName = org.apache.commons.lang.StringUtils.EMPTY;
    if (!note.getParentPath().equals("/")) {
      newName = note.getParentPath() + "/" + name;
    } else {
      if (!name.startsWith("/")) {
        newName = "/" + name;
      }
    }
    note.setCronSupported(notebook.getConf());
    notebook.moveNote(note.getId(), newName);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));

    final List<NoteInfo> notesInfo = notebook.getNotesInfo(getServiceContext().getUserAndRoles());
    connectionManager.broadcast( new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo));

    return new JsonResponse(HttpStatus.OK, "").build();
  }

  /**
   * Insert paragraph REST API.
   *
   * @param message - JSON containing paragraph's information
   */
  @ZeppelinApi
  @PostMapping(value = "/{noteId}/paragraph", produces = "application/json")
  public ResponseEntity insertParagraph(@PathVariable("noteId") final String noteId, final String message)
          throws IOException {
    final String user = securityService.getPrincipal();
    LOG.info("insert paragraph {} {}", noteId, message);

    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanWrite(noteId, "Insufficient privileges you cannot add paragraph to this note");

    final NewParagraphRequest request = NewParagraphRequest.fromJson(message);
    final AuthenticationInfo subject = new AuthenticationInfo(user);
    final Paragraph p;
    final Double indexDouble = request.getIndex();
    if (indexDouble == null) {
      p = note.addNewParagraph(subject);
    } else {
      p = note.insertNewParagraph(indexDouble.intValue(), subject);
    }
    initParagraph(p, request, user);
    notebook.saveNote(note);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
    return new JsonResponse(HttpStatus.OK, "", p.getId()).build();
  }


  /**
   * Get paragraph REST API.
   *
   * @param noteId ID of Note
   * @return JSON with information of the paragraph
   */
  @ZeppelinApi
  @GetMapping(value = "/{noteId}/paragraph/{paragraphId}", produces = "application/json")
  public ResponseEntity getParagraph(@PathVariable("noteId") final String noteId,
                                     @PathVariable("paragraphId") final String paragraphId) {
    LOG.info("get paragraph {} {}", noteId, paragraphId);

    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get this paragraph");
    final Paragraph p = note.getParagraph(paragraphId);
    checkIfParagraphIsNotNull(p);

    return new JsonResponse(HttpStatus.OK, "", p).build();
  }

  /**
   * Update paragraph.
   *
   * @param message json containing the "text" and optionally the "title" of the paragraph, e.g.
   *                {"text" : "updated text", "title" : "Updated title" }
   */
  //TODO(KOT): FIX
  @ZeppelinApi
  @PutMapping(value = "/{noteId}/paragraph/{paragraphId}", produces = "application/json")
  public ResponseEntity updateParagraph(@PathVariable("noteId") final String noteId,
                                        @PathVariable("paragraphId") final String paragraphId,
                                        final String message) throws IOException {
    final String user = securityService.getPrincipal();
    LOG.info("{} will update paragraph {} {}", user, noteId, paragraphId);

    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanWrite(noteId, "Insufficient privileges you cannot update this paragraph");
    final Paragraph p = note.getParagraph(paragraphId);
    checkIfParagraphIsNotNull(p);

    final UpdateParagraphRequest updatedParagraph = gson.fromJson(message, UpdateParagraphRequest.class);
    p.setText(updatedParagraph.getText());

    if (updatedParagraph.getTitle() != null) {
      p.setTitle(updatedParagraph.getTitle());
    }

    notebook.saveNote(note);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.PARAGRAPH).put("paragraph", p));
    return new JsonResponse(HttpStatus.OK, "").build();
  }


  @ZeppelinApi
  @PutMapping(value = "/{noteId}/paragraph/{paragraphId}/config", produces = "application/json")
  public ResponseEntity updateParagraphConfig(@PathVariable("noteId") final String noteId,
                                              @PathVariable("paragraphId") final String paragraphId,
                                              final String message) throws IOException {
    final String user = securityService.getPrincipal();
    LOG.info("{} will update paragraph config {} {}", user, noteId, paragraphId);

    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanWrite(noteId, "Insufficient privileges you cannot update this paragraph config");
    final Paragraph p = note.getParagraph(paragraphId);
    checkIfParagraphIsNotNull(p);

    final Map<String, Object> newConfig = gson.fromJson(message, HashMap.class);
    configureParagraph(p, newConfig, user);
    notebook.saveNote(note);
    return new JsonResponse(HttpStatus.OK, "", p).build();
  }

  /**
   * Move paragraph REST API.
   */
  @ZeppelinApi
  @PostMapping(value = "/{noteId}/paragraph/{paragraphId}/move/{newIndex}", produces = "application/json")
  public ResponseEntity moveParagraph(@PathVariable("noteId") final String noteId,
                                @PathVariable("paragraphId") final String paragraphId,
                                @PathVariable("newIndex") final int index)
          throws IOException {
    LOG.info("move paragraph {} {} {}", noteId, paragraphId, index);

    final Note note = notebook.getNote(noteId);
    if (index > 0 && index >= note.getParagraphCount()) {
      throw new BadRequestException("newIndex " + index + " is out of bounds");
    }

    note.moveParagraph(paragraphId, index);
    notebook.saveNote(note);

    final SockMessage message = new SockMessage(Operation.PARAGRAPH_MOVED)
            .put("id", paragraphId)
            .put("index", index);
    connectionManager.broadcast(note.getId(), message);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));

    return new JsonResponse(HttpStatus.OK, "").build();

  }


  /**
   * Delete paragraph REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.OK
   * @throws IOException
   */
  @ZeppelinApi
  @DeleteMapping(value = "/{noteId}/paragraph/{paragraphId}", produces = "application/json")
  public ResponseEntity deleteParagraph(@PathVariable("noteId") final String noteId,
                                  @PathVariable("paragraphId") final String paragraphId) throws IOException {
    LOG.info("delete paragraph {} {}", noteId, paragraphId);

    final Note note = notebook.getNote(noteId);
    note.removeParagraph(getServiceContext().getAutheInfo().getUser(), paragraphId);
    notebook.saveNote(note);

    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));

    return new JsonResponse(HttpStatus.OK, "").build();
  }

  /**
   * Clear result of all paragraphs REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.ok
   */
  @ZeppelinApi
  @PutMapping(value = "/{noteId}/clear", produces = "application/json")
  public ResponseEntity clearAllParagraphOutput(@PathVariable("noteId") final String noteId)
          throws IOException {
    LOG.info("clear all paragraph output of note {}", noteId);

    final Note note = notebook.getNote(noteId);
    note.clearAllParagraphOutput();
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
  @ZeppelinApi
  @PostMapping(value = "/job/{noteId}", produces = "application/json")
  public ResponseEntity runNoteJobs(@PathVariable("noteId") final String noteId,
                                    @RequestParam("waitToFinish") final Boolean waitToFinish)
          throws IOException, IllegalArgumentException {
    final boolean blocking = waitToFinish == null || waitToFinish;
    LOG.info("run note jobs {} waitToFinish: {}", noteId, blocking);
    final Note note = notebook.getNote(noteId);
    final AuthenticationInfo subject = new AuthenticationInfo(securityService.getPrincipal());
    subject.setRoles(new LinkedList<>(securityService.getAssociatedRoles()));
    checkIfNoteIsNotNull(note);
    checkIfUserCanRun(noteId, "Insufficient privileges you cannot run job for this note");

    try {
      note.runAllParagraphs(subject, blocking);
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
  @ZeppelinApi
  @DeleteMapping(value = "/job/{noteId}", produces = "application/json")
  public ResponseEntity stopNoteJobs(@PathVariable("noteId") final String noteId) throws IllegalArgumentException {
    LOG.info("stop note jobs {} ", noteId);
    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRun(noteId, "Insufficient privileges you cannot stop this job for this note");

    for (final Paragraph p : note.getParagraphs()) {
      if (!p.isTerminated()) {
        p.abort();
      }
    }
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
  @ZeppelinApi
  @GetMapping(value = "/job/{noteId}", produces = "application/json")
  public ResponseEntity getNoteJobStatus(@PathVariable("noteId") final String noteId)
          throws IOException, IllegalArgumentException {
    LOG.info("get note job status.");
    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get job status");

    return new JsonResponse(HttpStatus.OK, null, note.generateParagraphsInfo()).build();
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
  @ZeppelinApi
  @GetMapping(value = "/job/{noteId}/{paragraphId}", produces = "application/json")
  public ResponseEntity getNoteParagraphJobStatus(@PathVariable("noteId") final String noteId,
                                                  @PathVariable("paragraphId") final String paragraphId)
          throws IllegalArgumentException {
    LOG.info("get note paragraph job status.");
    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get job status");

    final Paragraph paragraph = note.getParagraph(paragraphId);
    checkIfParagraphIsNotNull(paragraph);

    return new JsonResponse(HttpStatus.OK, null, note.generateSingleParagraphInfo(paragraphId)).
            build();
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

    Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    Paragraph paragraph = note.getParagraph(paragraphId);
    checkIfParagraphIsNotNull(paragraph);

    Map<String, Object> params = new HashMap<>();
    if (!StringUtils.isEmpty(message)) {
      RunParagraphWithParametersRequest request =
              RunParagraphWithParametersRequest.fromJson(message);
      params = request.getParams();
    }
    notebookService.runParagraph(noteId, paragraphId, paragraph.getTitle(),
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

    Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    Paragraph paragraph = note.getParagraph(paragraphId);
    checkIfParagraphIsNotNull(paragraph);

    Map<String, Object> params = new HashMap<>();
    if (!StringUtils.isEmpty(message)) {
      RunParagraphWithParametersRequest request =
              RunParagraphWithParametersRequest.fromJson(message);
      params = request.getParams();
    }

    if (notebookService.runParagraph(noteId, paragraphId, paragraph.getTitle(),
            paragraph.getText(), params,
            new HashMap<>(), false, true, getServiceContext(), new RestServiceCallback<>())) {
      note = notebookService.getNote(noteId, getServiceContext(), new RestServiceCallback<>());
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
    notebookService.cancelParagraph(noteId, paragraphId, getServiceContext(),
            new RestServiceCallback<Paragraph>());
    return new JsonResponse(HttpStatus.OK).build();
  }
  */

  /**
   * Register cron job REST API.
   *
   * @param message - JSON with cron expressions.
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  @ZeppelinApi
  @PostMapping(value = "/cron/{noteId}", produces = "application/json")
  public ResponseEntity registerCronJob(@PathVariable("noteId") final String noteId, final String message)
          throws IllegalArgumentException {
    LOG.info("Register cron job note={} request cron msg={}", noteId, message);

    final CronRequest request = CronRequest.fromJson(message);

    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRun(noteId, "Insufficient privileges you cannot set a cron job for this note");
    checkIfNoteSupportsCron(note);

    if (!CronExpression.isValidExpression(request.getCronString())) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "wrong cron expressions.").build();
    }

    final Map<String, Object> config = note.getConfig();
    config.put("cron", request.getCronString());
    config.put("releaseresource", request.getReleaseResource());
    note.setConfig(config);
    notebook.refreshCron(note.getId());

    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Check valid cron expression REST API.
   *
   * @return JSON with status.OK
   * @throws IllegalArgumentException
   */
  @ZeppelinApi
  @PostMapping(value = "/cron/check_valid", produces = "application/json")
  public ResponseEntity checkCronExpression(@RequestParam("cronExpression") final String expression)
          throws IllegalArgumentException {
    if (!CronExpression.isValidExpression(expression)) {
      return new JsonResponse(HttpStatus.OK, "invalid").build();
    }
    return new JsonResponse(HttpStatus.OK, "valid").build();
  }

  /**
   * Remove cron job REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  @ZeppelinApi
  @DeleteMapping(value = "/cron/{noteId}", produces = "application/json")
  public ResponseEntity removeCronJob(@PathVariable("noteId") final String noteId)
          throws IOException, IllegalArgumentException {
    LOG.info("Remove cron job note {}", noteId);

    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserIsOwner(noteId,
            "Insufficient privileges you cannot remove this cron job from this note");
    checkIfNoteSupportsCron(note);

    final Map<String, Object> config = note.getConfig();
    config.remove("cron");
    config.remove("releaseresource");
    note.setConfig(config);
    notebook.refreshCron(note.getId());

    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Get cron job REST API.
   *
   * @param noteId ID of Note
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  @ZeppelinApi
  @GetMapping(value = "/cron/{noteId}", produces = "application/json")
  public ResponseEntity getCronJob(@PathVariable("noteId") final String noteId)
          throws IOException, IllegalArgumentException {
    LOG.info("Get cron job note {}", noteId);

    final Note note = notebook.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get cron information");
    checkIfNoteSupportsCron(note);
    final Map<String, Object> response = new HashMap<>();
    response.put("cron", note.getConfig().get("cron"));
    response.put("releaseResource", note.getConfig().get("releaseresource"));

    return new JsonResponse(HttpStatus.OK, response).build();
  }

  /**
   * Get note jobs for job manager.
   *
   * @return JSON with status.OK
   * @throws IOException
   * @throws IllegalArgumentException
   */
  //TODO(KOT): FIX
  @ZeppelinApi
  @GetMapping(value = "/jobmanager/", produces = "application/json")
  public ResponseEntity getJobListforNote() throws IOException, IllegalArgumentException {
    LOG.info("Get note jobs for job manager");
    final List<JobManagerService.NoteJobInfo> noteJobs = jobManagerService.getNoteJobInfoByUnixTime(0);
    final Map<String, Object> response = new HashMap<>();
    response.put("lastResponseUnixTime", System.currentTimeMillis());
    response.put("jobs", noteJobs);
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
  @ZeppelinApi
  @GetMapping(value = "/jobmanager/{lastUpdateUnixtime}/", produces = "application/json")
  public ResponseEntity getUpdatedJobListforNote(@PathVariable("lastUpdateUnixtime") long lastUpdateUnixTime)
          throws IOException, IllegalArgumentException {
    LOG.info("Get updated note jobs lastUpdateTime {}", lastUpdateUnixTime);
    List<JobManagerService.NoteJobInfo> noteJobs = jobManagerService.getNoteJobInfoByUnixTime(lastUpdateUnixTime);
    Map<String, Object> response = new HashMap<>();
    response.put("lastResponseUnixTime", System.currentTimeMillis());
    response.put("jobs", noteJobs);
    return new JsonResponse(HttpStatus.OK, response).build();
  }

  /**
   * Search for a Notes with permissions.
   */
  @ZeppelinApi
  @GetMapping(value = "/search", produces = "application/json")
  public ResponseEntity search(@RequestParam("q") final String queryTerm) {
    LOG.info("Searching notes for: {}", queryTerm);
    final String principal = securityService.getPrincipal();
    final Set<String> roles = securityService.getAssociatedRoles();
    final HashSet<String> userAndRoles = new HashSet<>();
    userAndRoles.add(principal);
    userAndRoles.addAll(roles);
    final List<Map<String, String>> notesFound = noteSearchService.query(queryTerm);
    for (int i = 0; i < notesFound.size(); i++) {
      final String[] ids = notesFound.get(i).get("id").split("/", 2);
      final String noteId = ids[0];
      if (!notebookAuthorization.isOwner(noteId, userAndRoles) &&
              !notebookAuthorization.isReader(noteId, userAndRoles) &&
              !notebookAuthorization.isWriter(noteId, userAndRoles) &&
              !notebookAuthorization.isRunner(noteId, userAndRoles)) {
        notesFound.remove(i);
        i--;
      }
    }
    LOG.info("{} notes found", notesFound.size());
    return new JsonResponse(HttpStatus.OK, notesFound).build();
  }

  private void initParagraph(final Paragraph p, final NewParagraphRequest request, final String user) {
    LOG.info("Init Paragraph for user {}", user);
    checkIfParagraphIsNotNull(p);
    p.setTitle(request.getTitle());
    p.setText(request.getText());
    final Map<String, Object> config = request.getConfig();
    if (config != null && !config.isEmpty()) {
      configureParagraph(p, config, user);
    }
  }

  private void configureParagraph(final Paragraph p, final Map<String, Object> newConfig, final String user) {
    LOG.info("Configure Paragraph for user {}", user);
    if (newConfig == null || newConfig.isEmpty()) {
      LOG.warn("{} is trying to update paragraph {} of note {} with empty config",
              user, p.getId(), p.getNote().getId());
      throw new BadRequestException("paragraph config cannot be empty");
    }
    final Map<String, Object> origConfig = p.getConfig();
    for (final Map.Entry<String, Object> entry : newConfig.entrySet()) {
      origConfig.put(entry.getKey(), entry.getValue());
    }

    p.setConfig(origConfig);
  }
}
