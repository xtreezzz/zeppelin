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

import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.notebook.repo.api.dto.display.InputDTO;

public class InputConverter {

  private InputConverter() {
    // not called
  }

  public static InputDTO convertToDTO(final Input object) {
    return new InputDTO<>(object.getName(), object.getDisplayName(), object.getDefaultValue(),
        object.isHidden(), object.getArgument());
  }

  public static Input convertFromDTOToObject(final InputDTO dto) {
    Input input = new Input();
    input.setArgument(dto.getArgument());
    input.setDisplayName(dto.getDisplayName());
    input.setHidden(dto.isHidden());
    input.setDefaultValue(dto.getDefaultValue());
    return input;
  }
}
