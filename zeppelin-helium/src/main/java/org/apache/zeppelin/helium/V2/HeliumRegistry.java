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
package org.apache.zeppelin.helium.V2;

public class HeliumRegistry {

  public static class HeliumPackage {
    String type;
    String name;
    String description;
    String artifact;
    String license;
    String icon;
    String published;

    public HeliumPackage(final String type,
                         final String name,
                         final String description,
                         final String artifact,
                         final String license,
                         final String icon,
                         final String published) {
      this.type = type;
      this.name = name;
      this.description = description;
      this.artifact = artifact;
      this.license = license;
      this.icon = icon;
      this.published = published;
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

    public String getLicense() {
      return license;
    }

    public String getIcon() {
      return icon;
    }

    public String getPublished() {
      return published;
    }
  }

  String registry;
  HeliumPackage pkg;
  boolean enabled;


  public HeliumRegistry(final String registry,
                        String type,
                        String name,
                        String description,
                        String artifact,
                        String license,
                        String icon,
                        String published,
                        final boolean enabled) {
    this.registry = registry;
    this.pkg = new HeliumPackage(type,
            name,
            description,
            artifact,
            license,
            icon,
            published);
    this.enabled = enabled;
  }

  public String getRegistry() {
    return registry;
  }

  public HeliumPackage getPkg() {
    return pkg;
  }

  public boolean isEnabled() {
    return enabled;
  }
}
