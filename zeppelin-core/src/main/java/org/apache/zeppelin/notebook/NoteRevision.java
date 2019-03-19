package org.apache.zeppelin.notebook;

import java.time.LocalDateTime;
import java.util.Objects;

public class NoteRevision {

  private long id;
  private final Long noteId;
  private final LocalDateTime date;
  private final String message;

  public NoteRevision(final long noteId, final LocalDateTime date, final String message) {
    this.noteId = noteId;
    this.date = date;
    this.message = message;
  }

  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public long getNoteId() {
    return noteId;
  }

  public LocalDateTime getDate() {
    return date;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NoteRevision)) {
      return false;
    }
    NoteRevision that = (NoteRevision) o;
    return Objects.equals(noteId, that.noteId) &&
        Objects.equals(date, that.date) &&
        Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(noteId, date, message);
  }
}
