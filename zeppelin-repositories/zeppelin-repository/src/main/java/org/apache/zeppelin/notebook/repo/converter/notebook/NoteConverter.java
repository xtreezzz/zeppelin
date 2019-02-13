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

package org.apache.zeppelin.notebook.repo.converter.notebook;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.repo.api.dto.display.AngularObjectDTO;
import org.apache.zeppelin.notebook.repo.api.dto.display.InputDTO;
import org.apache.zeppelin.notebook.repo.api.dto.notebook.NoteDTO;
import org.apache.zeppelin.notebook.repo.api.dto.notebook.ParagraphDTO;
import org.apache.zeppelin.notebook.repo.converter.display.AngularObjectConverter;
import org.apache.zeppelin.notebook.repo.converter.display.InputConverter;

public class NoteConverter {

  private NoteConverter() {
    // not called
  }

  public static NoteDTO convertToDTO(final Note object) {
    List<ParagraphDTO> paragraphDTOList = new ArrayList<>();
    object.getParagraphs().forEach(p -> paragraphDTOList.add(ParagraphConverter.convertToDTO(p)));

    Map<String, List<AngularObjectDTO>> angularDtoObjects = new HashMap<>();
    object.getAngularObjects().forEach((key, list) -> {
      List<AngularObjectDTO> angularObjectDTOList = new ArrayList<>();
      list.forEach(e -> angularObjectDTOList.add(AngularObjectConverter.convertToDTO(e)));
      angularDtoObjects.put(key, angularObjectDTOList);
    });

    Map<String, InputDTO> noteForms = new HashMap<>();
    object.getNoteForms().forEach((key, input) ->
        noteForms.put(key, InputConverter.convertToDTO(input)));

    return new NoteDTO(paragraphDTOList, object.getName(), object.getId(),
        object.getDefaultInterpreterGroup(), object.getNoteParams(), noteForms,
        angularDtoObjects, object.getConfig(), object.getInfo());
  }

  public static Note convertFromDTOToObject(final NoteDTO dto) {
    Map<String, List<AngularObject>> angularObjects = new HashMap<>();
    dto.getAngularObjects().forEach((key, list) -> {
      List<AngularObject> angularObjectList = new ArrayList<>();
      list.forEach(e -> angularObjectList.add(AngularObjectConverter.convertFromDTOToObject(e)));
      angularObjects.put(key, angularObjectList);
    });

    Map<String, Input> noteForms = new HashMap<>();
    dto.getNoteForms().forEach((key, input) ->
        noteForms.put(key, InputConverter.convertFromDTOToObject(input)));

    Note result = new Note();
    result.setId(dto.getId());
    result.setConfig(dto.getConfig());
    result.setDefaultInterpreterGroup(dto.getDefaultInterpreterGroup());
    result.setNoteParams(dto.getNoteParams());
    result.setNoteForms(noteForms);
    result.setInfo(dto.getInfo());
    result.setName(dto.getName());
    result.setAngularObjects(angularObjects);

    for (ParagraphDTO p : dto.getParagraphs()) {
      result.addParagraph(ParagraphConverter.convertFromDTOToObject(p));
    }
    return result;
  }
}
