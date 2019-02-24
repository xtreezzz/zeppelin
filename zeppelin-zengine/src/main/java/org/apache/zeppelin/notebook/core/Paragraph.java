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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.zeppelin.common.JsonSerializable;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.notebook.ApplicationState;
import org.apache.zeppelin.notebook.Note;

/**
 * Paragraph is a POJO which represents Note's sub-element.
 */
public class Paragraph implements Serializable, JsonSerializable {

  private String title;
  private String text;
  private String user;
  private Date dateUpdated;

  // paragraph configs like isOpen, colWidth, etc
  private Map<String, Object> config = new HashMap<>();

  // form and parameter settings
  private GUI settings = new GUI();
  private InterpreterResult results;

  // Application states in this paragraph
  private final List<ApplicationState> apps = new ArrayList<>();


  public Paragraph(String title, String text, String user, Date dateUpdated,
      Map<String, Object> config, GUI settings,
      InterpreterResult results) {
    this.title = title;
    this.text = text;
    this.user = user;
    this.dateUpdated = dateUpdated;
    this.config = config;
    this.settings = settings;
    this.results = results;
  }

  /**
   * Constructor for cloning paragraphs
   * @param other
   */
  public Paragraph(Paragraph other) {
    this.title = other.getTitle();
    this.text = other.getText();
    this.user = other.getUser();
    this.dateUpdated = other.getDateUpdated();
    this.config = other.getConfig();
    this.settings = other.getSettings();
    this.results = other.results;
    this.apps.addAll(other.getApps());
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
    this.dateUpdated = new Date();
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public Date getDateUpdated() {
    return dateUpdated;
  }

  public void setDateUpdated(Date dateUpdated) {
    this.dateUpdated = dateUpdated;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

  public GUI getSettings() {
    return settings;
  }

  public void setSettings(GUI settings) {
    this.settings = settings;
  }

  public InterpreterResult getResults() {
    return results;
  }

  public void setResults(InterpreterResult results) {
    this.results = results;
  }

  public List<ApplicationState> getApps() {
    synchronized (apps) {
      return Collections.unmodifiableList(apps);
    }
  }

  @Override
  public boolean equals(Object o) {
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
        .append(config, paragraph.config)
        .append(settings, paragraph.settings)
        .append(results, paragraph.results)
        .append(apps, paragraph.apps)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(title)
        .append(text)
        .append(user)
        .append(dateUpdated)
        .append(config)
        .append(settings)
        .append(results)
        .append(apps)
        .toHashCode();
  }


  public void addApplicationState(ApplicationState state) {
    apps.add(state);
  }

  public ApplicationState getApplicationState(String appId) {
    synchronized (apps) {
      for (ApplicationState as : apps) {
        if (as.getId().equals(appId)) {
          return as;
        }
      }
    }

    return null;
  }


  @Override
  public String toJson() {
    return Note.getGson().toJson(this);
  }

  public static Paragraph fromJson(String json) {
    return Note.getGson().fromJson(json, Paragraph.class);
  }
}