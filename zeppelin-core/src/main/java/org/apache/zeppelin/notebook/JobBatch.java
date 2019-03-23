package org.apache.zeppelin.notebook;

import java.time.LocalDateTime;
import java.util.LinkedList;

public class JobBatch {

  public enum Status {
    SAVING,
    PENDING,
    RUNNING,
    DONE,
    HANDLE_ERROR,
    ERROR,
    ABORTING,
    ABORTED,
    ;
  }

  private long id;
  private long noteId;
  private Status status;
  private LocalDateTime createdAt;
  private LocalDateTime startedAt;
  private LocalDateTime endedAt;


  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public long getNoteId() {
    return noteId;
  }

  public void setNoteId(final long noteId) {
    this.noteId = noteId;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(final Status status) {
    this.status = status;
  }

  public LocalDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(LocalDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public LocalDateTime getStartedAt() {
    return startedAt;
  }

  public void setStartedAt(LocalDateTime startedAt) {
    this.startedAt = startedAt;
  }

  public LocalDateTime getEndedAt() {
    return endedAt;
  }

  public void setEndedAt(LocalDateTime endedAt) {
    this.endedAt = endedAt;
  }
}
