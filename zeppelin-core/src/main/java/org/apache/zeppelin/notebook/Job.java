package org.apache.zeppelin.notebook;

import java.time.LocalDateTime;
import java.util.StringJoiner;

public class Job {

  public enum Status {
    PENDING,
    RUNNING,
    DONE,
    ERROR,
    CANCELED,
    ABORTING,
    ABORTED
    ;
  }

  private long id;
  private long batchId;
  private long noteId;
  private long paragpaphId;
  private long index;
  private String shebang;
  private Status status;
  private String interpreterProcessUUID;
  private String interpreterJobUUID;

  private LocalDateTime createdAt;
  private LocalDateTime startedAt;
  private LocalDateTime endedAt;

  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public long getBatchId() {
    return batchId;
  }

  public void setBatchId(final long batchId) {
    this.batchId = batchId;
  }

  public long getNoteId() {
    return noteId;
  }

  public void setNoteId(final long noteId) {
    this.noteId = noteId;
  }

  public long getParagpaphId() {
    return paragpaphId;
  }

  public void setParagpaphId(final long paragpaphId) {
    this.paragpaphId = paragpaphId;
  }

  public long getIndex() {
    return index;
  }

  public void setIndex(final long index) {
    this.index = index;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(final Status status) {
    this.status = status;
  }

  public String getInterpreterProcessUUID() {
    return interpreterProcessUUID;
  }

  public void setInterpreterProcessUUID(final String interpreterProcessUUID) {
    this.interpreterProcessUUID = interpreterProcessUUID;
  }

  public String getShebang() {
    return shebang;
  }

  public void setShebang(String shebang) {
    this.shebang = shebang;
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

  public String getInterpreterJobUUID() {
    return interpreterJobUUID;
  }

  public void setInterpreterJobUUID(String interpreterJobUUID) {
    this.interpreterJobUUID = interpreterJobUUID;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("id=" + id)
        .add("batchId=" + batchId)
        .add("noteId=" + noteId)
        .add("paragpaphId=" + paragpaphId)
        .add("index=" + index)
        .add("shebang='" + shebang + "'")
        .add("status=" + status)
        .add("interpreterProcessUUID='" + interpreterProcessUUID + "'")
        .add("interpreterJobUUID='" + interpreterJobUUID + "'")
        .add("createdAt=" + createdAt)
        .add("startedAt=" + startedAt)
        .add("endedAt=" + endedAt)
        .toString();
  }
}
