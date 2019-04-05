package org.apache.zeppelin;

import org.apache.zeppelin.externalDTO.ParagraphDTO;

import java.util.concurrent.ConcurrentLinkedQueue;

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
    paragraphEvents.add(new Event(noteId, before, after));
  }
}
