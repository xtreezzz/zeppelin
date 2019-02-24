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

package org.apache.zeppelin.notebook;

import org.apache.zeppelin.scheduler.Job;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An special NoteEventListener which handle events asynchronously
 */
public abstract class NoteEventAsyncListener implements NoteEventListener {

  private BlockingQueue<NoteEvent> eventsQueue = new LinkedBlockingQueue<>();

  private Thread eventHandlerThread;

  public NoteEventAsyncListener(String name) {
    this.eventHandlerThread = new EventHandlingThread();
    this.eventHandlerThread.setName(name);
    this.eventHandlerThread.start();
  }

  public abstract void handleNoteCreateEvent(NoteCreateEvent noteCreateEvent);

  public abstract void handleNoteRemoveEvent(NoteRemoveEvent noteRemoveEvent);

  public abstract void handleNoteUpdateEvent(NoteUpdateEvent noteUpdateEvent);

  public abstract void handleParagraphCreateEvent(ParagraphCreateEvent paragraphCreateEvent);

  public abstract void handleParagraphRemoveEvent(ParagraphRemoveEvent paragraphRemoveEvent);

  public abstract void handleParagraphUpdateEvent(ParagraphUpdateEvent paragraphUpdateEvent);


  public void close() {
    this.eventHandlerThread.interrupt();
  }

  @Override
  public void onNoteCreate(Note note) {
    eventsQueue.add(new NoteCreateEvent(note));
  }

  @Override
  public void onNoteRemove(Note note) {
    eventsQueue.add(new NoteRemoveEvent(note));
  }

  @Override
  public void onNoteUpdate(Note note) {
    eventsQueue.add(new NoteUpdateEvent(note));
  }

  @Override
  public void onParagraphCreate(ParagraphJob p) {
    eventsQueue.add(new ParagraphCreateEvent(p));
  }

  @Override
  public void onParagraphRemove(ParagraphJob p) {
    eventsQueue.add(new ParagraphRemoveEvent(p));
  }

  @Override
  public void onParagraphUpdate(ParagraphJob p) {
    eventsQueue.add(new ParagraphUpdateEvent(p));
  }

  @Override
  public void onParagraphStatusChange(ParagraphJob p, Job.Status status) {
    eventsQueue.add(new ParagraphStatusChangeEvent(p));
  }

  class EventHandlingThread extends Thread {

    @Override
    public void run() {
      while(!Thread.interrupted()) {
        try {
          NoteEvent event = eventsQueue.take();
          if (event instanceof NoteCreateEvent) {
            handleNoteCreateEvent((NoteCreateEvent) event);
          } else if (event instanceof NoteRemoveEvent) {
            handleNoteRemoveEvent((NoteRemoveEvent) event);
          } else if (event instanceof NoteUpdateEvent) {
            handleNoteUpdateEvent((NoteUpdateEvent) event);
          } else if (event instanceof ParagraphCreateEvent) {
            handleParagraphCreateEvent((ParagraphCreateEvent) event);
          } else if (event instanceof ParagraphRemoveEvent) {
            handleParagraphRemoveEvent((ParagraphRemoveEvent) event);
          } else if (event instanceof ParagraphUpdateEvent) {
            handleParagraphUpdateEvent((ParagraphUpdateEvent) event);
          } else {
            throw new RuntimeException("Unknown event: " + event.getClass().getSimpleName());
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Used for testing
   *
   * @throws InterruptedException
   */
  public void drainEvents() throws InterruptedException {
    while(!eventsQueue.isEmpty()) {
      Thread.sleep(1000);
    }
    Thread.sleep(5000);
  }

  interface NoteEvent {

  }

  public static class NoteCreateEvent implements NoteEvent {
    private Note note;

    public NoteCreateEvent(Note note) {
      this.note = note;
    }

    public Note getNote() {
      return note;
    }
  }

  public static class NoteUpdateEvent implements NoteEvent {
    private Note note;

    public NoteUpdateEvent(Note note) {
      this.note = note;
    }

    public Note getNote() {
      return note;
    }
  }


  public static class NoteRemoveEvent implements NoteEvent {
    private Note note;

    public NoteRemoveEvent(Note note) {
      this.note = note;
    }

    public Note getNote() {
      return note;
    }
  }

  public static class ParagraphCreateEvent implements NoteEvent {
    private ParagraphJob p;

    public ParagraphCreateEvent(ParagraphJob p) {
      this.p = p;
    }

    public ParagraphJob getParagraph() {
      return p;
    }
  }

  public static class ParagraphUpdateEvent implements NoteEvent {
    private ParagraphJob p;

    public ParagraphUpdateEvent(ParagraphJob p) {
      this.p = p;
    }

    public ParagraphJob getParagraph() {
      return p;
    }
  }

  public static class ParagraphRemoveEvent implements NoteEvent {
    private ParagraphJob p;

    public ParagraphRemoveEvent(ParagraphJob p) {
      this.p = p;
    }

    public ParagraphJob getParagraph() {
      return p;
    }
  }

  public static class ParagraphStatusChangeEvent implements NoteEvent {
    private ParagraphJob p;

    public ParagraphStatusChangeEvent(ParagraphJob p) {
      this.p = p;
    }

    public ParagraphJob getParagraph() {
      return p;
    }
  }
}
