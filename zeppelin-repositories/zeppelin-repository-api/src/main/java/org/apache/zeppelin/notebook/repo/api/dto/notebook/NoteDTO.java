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
import java.util.List;
import java.util.Map;
import org.apache.zeppelin.notebook.repo.api.dto.display.AngularObjectDTO;
import org.apache.zeppelin.notebook.repo.api.dto.display.InputDTO;


/**
 * Immutable Data Transfer Object for Note.
 */
public final class NoteDTO implements Serializable {
  private final String name;
  private final String id;
  private final String defaultInterpreterGroup;
  private final Map<String, Object> noteParams;
  private final Map<String, InputDTO> noteForms;
  private final List<ParagraphDTO> paragraphs;
  private final Map<String, List<AngularObjectDTO>> angularObjects;
  private final Map<String, Object> config;
  private final Map<String, Object> info;

  public NoteDTO(List<ParagraphDTO> paragraphs, String name, String id,
      String defaultInterpreterGroup, Map<String, Object> noteParams,
      Map<String, InputDTO> noteForms,
      Map<String, List<AngularObjectDTO>> angularObjects,
      Map<String, Object> config, Map<String, Object> info) {
    this.paragraphs = paragraphs;
    this.name = name;
    this.id = id;
    this.defaultInterpreterGroup = defaultInterpreterGroup;
    this.noteParams = noteParams;
    this.noteForms = noteForms;
    this.angularObjects = angularObjects;
    this.config = config;
    this.info = info;
  }

  public List<ParagraphDTO> getParagraphs() {
    return paragraphs;
  }

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public String getDefaultInterpreterGroup() {
    return defaultInterpreterGroup;
  }

  public Map<String, Object> getNoteParams() {
    return noteParams;
  }

  public Map<String, InputDTO> getNoteForms() {
    return noteForms;
  }

  public Map<String, List<AngularObjectDTO>> getAngularObjects() {
    return angularObjects;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public Map<String, Object> getInfo() {
    return info;
  }
}
