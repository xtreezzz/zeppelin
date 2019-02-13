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

package org.apache.zeppelin.notebook.repo.converter.helium;

import org.apache.zeppelin.helium.HeliumPackage;
import org.apache.zeppelin.helium.HeliumType;
import org.apache.zeppelin.notebook.repo.api.dto.helium.HeliumPackageDTO;

public class HeliumPackageConverter {

  private HeliumPackageConverter() {
    // not called
  }

  public static HeliumPackageDTO convertToDTO(final HeliumPackage object) {
    return new HeliumPackageDTO(object.getType().name(), object.getName(),
        object.getDescription(), object.getArtifact(), object.getClassName(), object.getResources(),
        object.getLicense(), object.getIcon(), object.getPublishedDate(), object.getGroupId(),
        object.getArtifactId(), SpellPackageInfoConverter.convertToDTO(object.getSpellInfo()),
        object.getConfig());
  }

  public static HeliumPackage convertFromDTOToObject(final HeliumPackageDTO dto) {
    return new HeliumPackage(HeliumType.valueOf(dto.getType()), dto.getName(),
        dto.getDescription(), dto.getArtifact(), dto.getClassName(), dto.getResources(),
        dto.getLicense(), dto.getIcon());
  }
}