package org.apache.zeppelin.notebook;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Service for managing note's user permissions.
 *
 * @see Note
 */
@Component
public class NotePermissionsService {

  private static final Logger LOG = LoggerFactory.getLogger(NotePermissionsService.class);

  /*
   * Contains roles for each user.
   */
  private final Map<String, Set<String>> userRoles = new HashMap<>();
  private final NoteManager storage;


  @Autowired
  public NotePermissionsService(NoteManager storage) {
    this.storage = storage;
  }

  public void setUserRoles(final String user, final Set<String> roles) {
    if (StringUtils.isBlank(user)) {
      LOG.warn("Setting roles for empty user");
      return;
    }
    roles.retainAll(validateNames(roles));
    userRoles.put(user, roles);
  }

  public Set<String> getUserRoles(String user) {
    Set<String> roles = Sets.newHashSet();
    if (userRoles.containsKey(user)) {
      roles.addAll(userRoles.get(user));
    }
    return roles;
  }

  private boolean isValidName(final String name) {
    return !name.trim().isEmpty();
  }

  private Set<String> validateNames(final Set<String> users) {
    final Set<String> result = new HashSet<>();
    for (final String user : users) {
      if (isValidName(user)) {
        result.add(user.trim());
      }
    }
    return result;
  }

  public void setOwners(final String noteId, Set<String> entities) {
    try {
      Note target = storage.getNote(noteId);
      target.getOwners().addAll(validateNames(entities));
      storage.saveNote(target);
    } catch (IOException e) {
      LOG.error("Failed to set owners for note - {}", noteId, e);
    }
  }

  public void setReaders(final String noteId, Set<String> entities) {
    try {
      Note target = storage.getNote(noteId);
      target.getReaders().addAll(validateNames(entities));
      storage.saveNote(target);
    } catch (IOException e) {
      LOG.error("Failed to set readers for note - {}", noteId, e);
    }
  }

  public void setRunners(final String noteId, Set<String> entities) {
    try {
      Note target = storage.getNote(noteId);
      target.getRunners().addAll(validateNames(entities));
      storage.saveNote(target);
    } catch (IOException e) {
      LOG.error("Failed to set runners for note - {}", noteId, e);
    }
  }

  public void setWriters(final String noteId, Set<String> entities) {
    try {
      Note target = storage.getNote(noteId);
      target.getWriters().addAll(validateNames(entities));
      storage.saveNote(target);
    } catch (IOException e) {
      LOG.error("Failed to set writers for note - {}", noteId, e);
    }
  }

  /**
   *
   * @param noteId
   * @param entities
   * @return true if user is owner or admin, false otherwise or if note is not exist.
   */
  public boolean isOwner(final String noteId, final Set<String> entities) {
    try {
      Note target = storage.getNote(noteId);
      if (target != null) {
        return isMember(entities, target.getOwners()) || isAdmin(entities);
      }
    } catch (IOException e) {
      LOG.error("Failed to get note by id - {}", noteId, e);
    }
    return false;
  }

  /**
   *
   * @param noteId
   * @param entities
   * @return true if user is writer or owner or admin, false otherwise or if note is not exist.
   */
  public boolean isWriter(final String noteId, final Set<String> entities) {
    try {
      Note target = storage.getNote(noteId);
      if (target != null) {
        return isMember(entities, target.getWriters()) ||
            isMember(entities, target.getOwners()) ||
            isAdmin(entities);
      }
    } catch (IOException e) {
      LOG.error("Failed to get note by id - {}", noteId, e);
    }
    return false;
  }

  /**
   *
   * @param noteId
   * @param entities
   * @return true if user is reader or owner or writer or runner or admin,
   * false otherwise or if note is not exist.
   */
  public boolean isReader(final String noteId, final Set<String> entities) {
    try {
      Note target = storage.getNote(noteId);
      if (target != null) {
        return isMember(entities, target.getReaders()) ||
            isMember(entities, target.getOwners()) ||
            isMember(entities, target.getWriters()) ||
            isMember(entities, target.getRunners()) ||
            isAdmin(entities);
      }
    } catch (IOException e) {
      LOG.error("Failed to get note by id - {}", noteId, e);
    }
    return false;
  }

  /**
   *
   * @param noteId
   * @param entities
   * @return true if user is runner it writer or owner or admin,
   * false otherwise or if note is not exist.
   */
  public boolean isRunner(final String noteId, final Set<String> entities) {
    try {
      Note target = storage.getNote(noteId);
      if (target != null) {
        return isMember(entities, target.getRunners()) ||
            isMember(entities, target.getWriters()) ||
            isMember(entities, target.getOwners()) ||
            isAdmin(entities);
      }
    } catch (IOException e) {
      LOG.error("Failed to get note by id - {}", noteId, e);
    }
    return false;
  }

  private boolean isAdmin(final Set<String> entities) {
    final String adminRole = ConfVars.ZEPPELIN_OWNER_ROLE.getStringValue();
    if (StringUtils.isBlank(adminRole)) {
      return false;
    }
    return entities.contains(adminRole);
  }

  /**
   * @return  true if b is empty or if (a intersection b) is non-empty
   */
  private boolean isMember(final Set<String> a, final Set<String> b) {
    final Set<String> intersection = new HashSet<>(b);
    intersection.retainAll(a);
    return (b.isEmpty() || !intersection.isEmpty());
  }

  private boolean isAnonymousAllowed() {
    return ConfVars.ZEPPELIN_ANONYMOUS_ALLOWED.getBooleanValue();
  }

  private boolean isNotebookPublic() {
    return ConfVars.ZEPPELIN_NOTEBOOK_PUBLIC.getBooleanValue();
  }

  public boolean isOwner(final Set<String> userAndRoles, final String noteId) {
    if (isAnonymousAllowed()) {
      LOG.debug("Zeppelin runs in anonymous mode, everybody is owner");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isOwner(noteId, userAndRoles);
  }

  public boolean hasWriteAuthorization(final Set<String> userAndRoles, final String noteId) {
    if (isAnonymousAllowed()) {
      LOG.debug("Zeppelin runs in anonymous mode, everybody is writer");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isWriter(noteId, userAndRoles);
  }

  public boolean hasReadAuthorization(final Set<String> userAndRoles, final String noteId) {
    if (isAnonymousAllowed()) {
      LOG.debug("Zeppelin runs in anonymous mode, everybody is reader");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isReader(noteId, userAndRoles);
  }

  public boolean hasRunAuthorization(final Set<String> userAndRoles, final String noteId) {
    if (isAnonymousAllowed()) {
      LOG.debug("Zeppelin runs in anonymous mode, everybody is runner");
      return true;
    }
    if (userAndRoles == null) {
      return false;
    }
    return isRunner(noteId, userAndRoles);
  }

  public void addNewUser(final String noteId, final String user) throws IOException {
    Note target = null;
    try {
      target = storage.getNote(noteId);
    } catch (IOException e) {
      LOG.error("Failed to get note by id - {}", noteId, e);
    }
    if (target != null) {
      if (isNotebookPublic()) {
        // add current user to owners - can be public
        final Set<String> owners = target.getOwners();
        owners.add(user);
        setOwners(noteId, owners);
      } else {
        // add current user to owners, readers, runners, writers - private note
        if (isValidName(user)) {
          target.getOwners().add(user);
          target.getReaders().add(user);
          target.getRunners().add(user);
          target.getWriters().add(user);
          storage.saveNote(target);
        }
      }
    }
  }
}
