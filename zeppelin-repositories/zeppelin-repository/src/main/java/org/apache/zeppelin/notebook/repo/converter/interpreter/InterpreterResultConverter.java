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

package org.apache.zeppelin.notebook.repo.converter.interpreter;

import java.util.ArrayList;
import java.util.List;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;
import org.apache.zeppelin.notebook.repo.api.dto.interpreter.InterpreterResultDTO;
import org.apache.zeppelin.notebook.repo.api.dto.interpreter.InterpreterResultMessageDTO;

public class InterpreterResultConverter {

  private InterpreterResultConverter() {
    // not called
  }

  public static InterpreterResultDTO convertToDTO(final InterpreterResult object) {
    List<InterpreterResultMessageDTO> messageDTOList= new ArrayList<>();
    object.message().forEach(m ->
        messageDTOList.add(InterpreterResultMessageConverter.convertToDTO(m)));
    return new InterpreterResultDTO(object.code().name(), messageDTOList);
  }

  public static InterpreterResult convertFromDTOToObject(final InterpreterResultDTO dto) {
    List<InterpreterResultMessage> msgs = new ArrayList<>();
    dto.getMsg().forEach(m ->
        msgs.add(InterpreterResultMessageConverter.convertFromDTOToObject(m)));
    return new InterpreterResult(Code.valueOf(dto.getCode()), msgs);
  }
}
