package ru.tinkoff.zeppelin.storage;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Represents System Event with eventType.
 */
public class SystemEventDTO implements Serializable {

  @Nonnull
  private String username;

  @Nonnull
  private String eventType;

  @Nonnull
  private String message;

  @Nullable
  private String description;

  @Nonnull
  private LocalDateTime actionTime;


  public SystemEventDTO(@Nonnull final String username,
                        @Nonnull final String eventType,
                        @Nonnull final String message,
                        @Nullable final String description,
                        @Nonnull final LocalDateTime actionTime) {
    this.username = username;
    this.eventType = eventType;
    this.message = message;
    this.description = description;
    this.actionTime = actionTime;
  }

  @Nonnull
  public String getUsername() {
    return username;
  }

  public void setUsername(@Nonnull final String username) {
    this.username = username;
  }

  @Nonnull
  public String getEventType() {
    return eventType;
  }

  public void setEventType(@Nonnull final String eventType) {
    this.eventType = eventType;
  }

  @Nonnull
  public String getMessage() {
    return message;
  }

  public void setMessage(@Nonnull final String message) {
    this.message = message;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  public void setDescription(@Nullable final String description) {
    this.description = description;
  }

  @Nonnull
  public LocalDateTime getActionTime() {
    return actionTime;
  }

  public void setActionTime(@Nonnull final LocalDateTime actionTime) {
    this.actionTime = actionTime;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("username='" + username + "'")
        .add("eventType='" + eventType + "'")
        .add("message='" + message + "'")
        .add("description='" + description + "'")
        .add("actionTime=" + actionTime)
        .toString();
  }
}
