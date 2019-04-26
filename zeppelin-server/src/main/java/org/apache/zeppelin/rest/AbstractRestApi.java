package org.apache.zeppelin.rest;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.exception.ForbiddenException;
import org.apache.zeppelin.rest.exception.NoteNotFoundException;
import org.apache.zeppelin.rest.exception.ParagraphNotFoundException;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.handler.AbstractHandler.Permission;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteService;


abstract class AbstractRestApi {

  protected final NoteService noteService;
  protected final ConnectionManager connectionManager;

  protected AbstractRestApi(
      final NoteService noteService,
      final ConnectionManager connectionManager) {
    this.noteService = noteService;
    this.connectionManager = connectionManager;
  }

  Note secureLoadNote(final long noteId, final Permission permission) {
    Note note = noteService.getNote(noteId);

    if (note == null) {
      throw new NoteNotFoundException("Can't find note with id " + noteId);
    }

    boolean isAllowed = false;
    Set<String> allowed = null;
    switch (permission) {
      case READER:
        isAllowed = userHasReaderPermission(note);
        allowed = note.getReaders();
        break;
      case WRITER:
        isAllowed = userHasWriterPermission(note);
        allowed = note.getWriters();
        break;
      case RUNNER:
        isAllowed = userHasRunnerPermission(note);
        allowed = note.getRunners();
        break;
      case OWNER:
        isAllowed = userHasOwnerPermission(note);
        allowed = note.getOwners();
        break;
    }
    if (!isAllowed) {
      final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
      final String errorMsg = "Insufficient privileges to " + permission + " note.\n" +
          "Allowed users or roles: " + allowed + "\n" + "But the user " +
          authenticationInfo.getUser() + " belongs to: " + authenticationInfo.getRoles();
      throw new ForbiddenException(errorMsg);
    }
    return note;
  }

  Paragraph getParagraph(final Note note, final Long paragraphId) {
    return noteService.getParagraphs(note).stream()
        .filter(p -> p.getId().equals(paragraphId))
        .findAny()
        .orElseThrow(() -> new ParagraphNotFoundException("paragraph not found"));
  }

  <T> void updateIfNotNull(final Supplier<T> getter, final Consumer<T> setter) {
    T requestParam = getter.get();
    if (requestParam != null) {
      setter.accept(requestParam);
    }
  }

  private boolean userHasAdminPermission() {
    final Set<String> admin = new HashSet<>();
    admin.addAll(Configuration.getAdminUsers());
    admin.addAll(Configuration.getAdminGroups());
    return userRolesContains(admin);
  }

  boolean userHasOwnerPermission(final Note note) {
    return userRolesContains(note.getOwners()) || userHasAdminPermission();
  }

  boolean userHasWriterPermission(final Note note) {
    return userRolesContains(note.getWriters()) || userHasAdminPermission();
  }

  boolean userHasRunnerPermission(final Note note) {
    return userRolesContains(note.getRunners()) || userHasAdminPermission();
  }

  boolean userHasReaderPermission(final Note note) {
    return userRolesContains(note.getReaders()) || userHasAdminPermission();
  }

  private static Set<String> getUserAvailableRoles() {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    Set<String> userRoles = new HashSet<>();
    userRoles.add(authenticationInfo.getUser());
    userRoles.addAll(authenticationInfo.getRoles());
    return userRoles;
  }

  private boolean userRolesContains(final Set<String> neededRoles) {
    for (String availableRole : getUserAvailableRoles()) {
      if (neededRoles.contains(availableRole)) {
        return true;
      }
    }
    return false;
  }
}