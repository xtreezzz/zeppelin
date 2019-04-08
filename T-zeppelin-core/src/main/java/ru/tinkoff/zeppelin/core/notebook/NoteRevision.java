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
