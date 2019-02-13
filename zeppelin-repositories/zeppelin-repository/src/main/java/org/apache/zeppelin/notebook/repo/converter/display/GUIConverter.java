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

package org.apache.zeppelin.notebook.repo.converter.display;

import java.util.HashMap;
import java.util.Map;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.notebook.repo.api.dto.display.GUIDTO;
import org.apache.zeppelin.notebook.repo.api.dto.display.InputDTO;

public class GUIConverter{

  private GUIConverter() {
    // not called
  }

  public static GUIDTO convertToDTO(final GUI object) {
    Map<String, InputDTO> forms = new HashMap<>();
    object.getForms().forEach((key, value) -> forms.put(key, InputConverter.convertToDTO(value)));

    return new GUIDTO(object.getParams(), forms);
  }

  public static GUI convertFromDTOToObject(final GUIDTO dto) {
    Map<String, Input> forms = new HashMap<>();
    dto.getForms().forEach((key, value) ->
        forms.put(key, InputConverter.convertFromDTOToObject(value)));

    GUI result = new GUI();
    result.setParams(dto.getParams());
    result.setForms(forms);
    return result;
  }
}
