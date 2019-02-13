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

package org.apache.zeppelin.notebook.repo.api.dto.helium;

import java.io.Serializable;
import java.util.Map;

/**
 * Immutable Data Transfer Object for HeliumPackage.
 */
public final class HeliumPackageDTO implements Serializable {

  private final String type;
  private final String name;           // user friendly name of this application
  private final String description;    // description
  private final String artifact;       // artifact name e.g) groupId:artifactId:versionId
  private final String className;      // entry point
  // resource classnames that requires [[ .. and .. and .. ] or [ .. and .. and ..] ..]
  private  String [][] resources;

  private final String license;
  private final String icon;
  private final String published;

  private final String groupId;        // get groupId of INTERPRETER type package
  private final String artifactId;     // get artifactId of INTERPRETER type package

  private final SpellPackageInfoDTO spell;
  private final Map<String, Object> config;

  public HeliumPackageDTO(
      String type, String name, String description, String artifact, String className,
      String[][] resources, String license, String icon, String published, String groupId,
      String artifactId, SpellPackageInfoDTO spell,
      Map<String, Object> config) {
    this.type = type;
    this.name = name;
    this.description = description;
    this.artifact = artifact;
    this.className = className;
    this.resources = resources;
    this.license = license;
    this.icon = icon;
    this.published = published;
    this.groupId = groupId;
    this.artifactId = artifactId;
    this.spell = spell;
    this.config = config;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getArtifact() {
    return artifact;
  }

  public String getClassName() {
    return className;
  }

  public String[][] getResources() {
    return resources;
  }

  public String getLicense() {
    return license;
  }

  public String getIcon() {
    return icon;
  }

  public String getPublished() {
    return published;
  }

  public String getGroupId() {
    return groupId;
  }

  public String getArtifactId() {
    return artifactId;
  }

  public SpellPackageInfoDTO getSpell() {
    return spell;
  }

  public Map<String, Object> getConfig() {
    return config;
  }
}
