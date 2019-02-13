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

package org.apache.zeppelin.notebook.repo.api.dto.notebook;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.zeppelin.notebook.repo.api.dto.display.GUIDTO;
import org.apache.zeppelin.notebook.repo.api.dto.interpreter.InterpreterResultDTO;


/**
 * Immutable Data Transfer Object for Paragraph.
 */
public final class ParagraphDTO implements Serializable {
  private final String title;
  // text is composed of intpText and scriptText.
  private final String text;
  private final String user;
  private final Date dateUpdated;
  // paragraph configs like isOpen, colWidth, etc
  private final Map<String, Object> config;
  // form and parameter settings
  private final GUIDTO settings;
  private final InterpreterResultDTO results;
  // Application states in this paragraph
  private final List<ApplicationStateDTO> apps;

  public ParagraphDTO(String title, String text, String user, Date dateUpdated,
      Map<String, Object> config, GUIDTO settings,
      InterpreterResultDTO results,
      List<ApplicationStateDTO> apps) {
    this.title = title;
    this.text = text;
    this.user = user;
    this.dateUpdated = dateUpdated;
    this.config = config;
    this.settings = settings;
    this.results = results;
    this.apps = apps;
  }

  public String getTitle() {
    return title;
  }

  public String getText() {
    return text;
  }

  public String getUser() {
    return user;
  }

  public Date getDateUpdated() {
    return dateUpdated;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public GUIDTO getSettings() {
    return settings;
  }

  public InterpreterResultDTO getResults() {
    return results;
  }

  public List<ApplicationStateDTO> getApps() {
    return apps;
  }
}
