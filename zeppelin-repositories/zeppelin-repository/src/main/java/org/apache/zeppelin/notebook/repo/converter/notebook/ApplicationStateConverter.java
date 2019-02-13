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

import org.apache.zeppelin.notebook.ApplicationState;
import org.apache.zeppelin.notebook.repo.api.dto.notebook.ApplicationStateDTO;
import org.apache.zeppelin.notebook.repo.converter.helium.HeliumPackageConverter;

public class ApplicationStateConverter {
  private ApplicationStateConverter() {
    // not called
  }

  public static ApplicationStateDTO convertToDTO(final ApplicationState object) {
    return new ApplicationStateDTO(object.getId(),
        HeliumPackageConverter.convertToDTO(object.getHeliumPackage()), object.getOutput());
  }

  public static ApplicationState convertFromDTOToObject(final ApplicationStateDTO dto) {
    ApplicationState state = new ApplicationState(dto.getId(),
        HeliumPackageConverter.convertFromDTOToObject(dto.getPkg()));
    state.setOutput(dto.getOutput());
    return state;
  }
}
