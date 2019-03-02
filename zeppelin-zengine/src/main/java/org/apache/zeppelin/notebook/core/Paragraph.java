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

package org.apache.zeppelin.notebook.core;

import java.io.Serializable;
import java.util.Date;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.notebook.Note;

/**
 * Paragraph is a POJO which represents Note's sub-element.
 */
public class Paragraph implements Serializable {

  //TODO(egorklimov):
  //  * Убрал конфиг, так как видимо в нем хранилось только isEnabled - думаю стоит вынести это в джобу
  //  * Убрал InterpreterResult - надо сделать сервис по загрузке
  //  * Убрал ApplicationState - надо сделать сервис по загрузке
  //  * Что делать с id? Нужно согласование с id ParagraphJob и Job

  private String title;
  private String text;
  private String user;
  private Date dateUpdated;

  // form and parameter settings
  private GUI settings;

  public Paragraph(final String title, final String text, final String user, final Date dateUpdated,
      final GUI settings) {
    this.title = title;
    this.text = text;
    this.user = user;
    this.dateUpdated = dateUpdated;
    this.settings = settings;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(final String title) {
    this.title = title;
  }

  public String getText() {
    return text;
  }

  public void setText(final String text) {
    this.text = text;
    this.dateUpdated = new Date();
  }

  public String getUser() {
    return user;
  }

  public void setUser(final String user) {
    this.user = user;
  }

  public Date getDateUpdated() {
    return dateUpdated;
  }

  public void setDateUpdated(final Date dateUpdated) {
    this.dateUpdated = dateUpdated;
  }

  public GUI getSettings() {
    return settings;
  }

  public void setSettings(final GUI settings) {
    this.settings = settings;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Paragraph paragraph = (Paragraph) o;

    return new EqualsBuilder()
        .append(title, paragraph.title)
        .append(text, paragraph.text)
        .append(user, paragraph.user)
        .append(dateUpdated, paragraph.dateUpdated)
        .append(settings, paragraph.settings)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(title)
        .append(text)
        .append(user)
        .append(dateUpdated)
        .append(settings)
        .toHashCode();
  }

  public static Paragraph fromJson(final String json) {
    return Note.getGson().fromJson(json, Paragraph.class);
  }
}