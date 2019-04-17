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
package ru.tinkoff.zeppelin.core.notebook;

import java.time.LocalDateTime;
import java.util.Set;
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
  private long paragraphId;
  private long index;
  private String shebang;
  private Status status;
  private String interpreterProcessUUID;
  private String interpreterJobUUID;
  private String username;
  private Set<String> roles;

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

  public long getParagraphId() {
    return paragraphId;
  }

  public void setParagraphId(final long paragpaphId) {
    this.paragraphId = paragpaphId;
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

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public void setRoles(Set<String> roles) {
    this.roles = roles;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("id=" + id)
        .add("batchId=" + batchId)
        .add("noteId=" + noteId)
        .add("paragraphId=" + paragraphId)
        .add("index=" + index)
        .add("shebang='" + shebang + "'")
        .add("status=" + status)
        .add("username=" + username)
        .add("roles=" + String.join(" ,", roles))
        .add("interpreterProcessUUID='" + interpreterProcessUUID + "'")
        .add("interpreterJobUUID='" + interpreterJobUUID + "'")
        .add("createdAt=" + createdAt)
        .add("startedAt=" + startedAt)
        .add("endedAt=" + endedAt)
        .toString();
  }
}
