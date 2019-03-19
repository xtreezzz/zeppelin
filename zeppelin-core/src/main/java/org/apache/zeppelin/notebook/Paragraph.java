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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.zeppelin.notebook.display.GUI;

import java.io.Serializable;
import java.security.SecureRandom;
import java.time.LocalDateTime;

/**
 * Paragraph is a POJO which represents Note's sub-element.
 */
public class Paragraph implements Serializable {

  //TODO(egorklimov):
  //  * Убрал конфиг, так как видимо в нем хранилось только isEnabled - думаю стоит вынести это в джобу
  //  * Убрал InterpreterResult - надо сделать сервис по загрузке
  //  * Убрал ApplicationState - надо сделать сервис по загрузке
  private long databaseId;
  private String id;
  private String title;
  private String text;
  private String user;
  private LocalDateTime created;
  private LocalDateTime updated;

  //TODO(SAN) вернул config :)
  //paragraph configs like isOpen, colWidth, etc
  private Map<String, Object> config;

  // form and parameter settings
  private GUI settings;

  public Paragraph(final String title,
      final String text,
      final String user,
      final GUI settings) {
    this.id = "paragraph_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt();
    this.title = title;
    this.text = text;
    this.user = user;
    this.created = LocalDateTime.now();
    this.updated = LocalDateTime.now();
    this.config = new HashMap<>();
    this.settings = settings;
  }

  public String getId() {
    return id;
  }

  public void setId(final String id) {
    this.id = id;
  }

  public long getDatabaseId() {
    return databaseId;
  }

  public void setDatabaseId(final long databaseId) {
    this.databaseId = databaseId;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(final String title) {
    this.title = title;
    this.updated = LocalDateTime.now();
  }

  public String getText() {
    return text;
  }

  public void setText(final String text) {
    this.text = text;
    this.updated = LocalDateTime.now();
  }

  public String getUser() {
    return user;
  }

  public void setUser(final String user) {
    this.user = user;
    this.updated = LocalDateTime.now();
  }

  public LocalDateTime getCreated() {
    return created;
  }

  public void setCreated(final LocalDateTime created) {
    this.created = created;
  }

  public LocalDateTime getUpdated() {
    return updated;
  }

  public void setUpdated(final LocalDateTime updated) {
    this.updated = updated;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public void setConfig(final Map<String, Object> config) {
    this.config = config;
  }

  public GUI getSettings() {
    return settings;
  }

  public void setSettings(final GUI settings) {
    this.settings = settings;
    this.updated = LocalDateTime.now();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Paragraph paragraph = (Paragraph) o;

    return new EqualsBuilder()
        .append(title, paragraph.title)
        .append(text, paragraph.text)
        .append(user, paragraph.user)
        .append(created, paragraph.created)
        .append(config, paragraph.config)
        .append(settings, paragraph.settings)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(title)
        .append(text)
        .append(user)
        .append(created)
        .append(config)
        .append(settings)
        .toHashCode();
  }

  // Много где вызывается, проще сделать такую заглушку
  public Note getNote() {
    throw new NotImplementedException("Parent note has been removed from paragraph");
  }

}