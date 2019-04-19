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
package ru.tinkoff.zeppelin.engine;

import ru.tinkoff.zeppelin.core.externalDTO.ParagraphDTO;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Base event handler
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
public class EventService {
  public enum Type {
    NOTE_CREATED,
    NOTE_UPDATED,
    NOTE_REMOVED,

    PARAGRAPH_ADDED,
    PARAGRAPH_UPDATED,
    PARAGRAPH_REMOVED,

    JOB_BATCH_STATUS_CHANGED,
    JOB_STATUS_CHANGED,
    JOB_RESULT_RECEIVED,
  }

  public static class Event {
    private final Long noteId;
    private final ParagraphDTO before;
    private final ParagraphDTO after;

    public Event(final Long noteId, final ParagraphDTO before, final ParagraphDTO after) {
      this.noteId = noteId;
      this.before = before;
      this.after = after;
    }

    public Long getNoteId() {
      return noteId;
    }

    public ParagraphDTO getBefore() {
      return before;
    }

    public ParagraphDTO getAfter() {
      return after;
    }
  }

  private static ConcurrentLinkedQueue<Event> paragraphEvents = new ConcurrentLinkedQueue<>();

  public static Event getEvent() {
    return paragraphEvents.poll();
  }

  public static void publish(Type type, Object ... args) {
  }

  public static void publish(final long noteId, final ParagraphDTO before, final ParagraphDTO after) {
    if(!Objects.equals(before, after)) {
      paragraphEvents.add(new Event(noteId, before, after));
    }
  }
}
