package org.apache.zeppelin;

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

  public static void publish(final Type type, final String noteUUID, Object... args) {

  }

  public static void getEvent() {

  }

  public static void publish(Type type, Object ... args) {

  }
}
