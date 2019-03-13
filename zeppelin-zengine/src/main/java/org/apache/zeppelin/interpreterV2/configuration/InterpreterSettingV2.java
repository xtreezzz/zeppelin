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

package org.apache.zeppelin.interpreterV2.configuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.zeppelin.Dependency;
import org.apache.zeppelin.utils.IdHashes;

import java.io.Serializable;
import java.util.*;


/**
 * Represent one InterpreterSetting in the interpreter setting page
 */
public class InterpreterSettingV2 implements Serializable {

  /***
   * Interpreter status
   */
  public enum Status {
    DOWNLOADING_DEPENDENCIES,
    ERROR,
    READY
  }

  private String id;
  private String name;
  // the original interpreter setting template name where it is created from
  private String group;

  /**
   * properties can be either Properties or Map<String, InterpreterPropertyOld>
   * properties should be:
   * - Properties when Interpreter instances are saved to `conf/interpreter.json` file
   * - Map<String, InterpreterPropertyOld> when Interpreters are registered
   * : this is needed after https://github.com/apache/zeppelin/pull/1145
   * which changed the way of getting default interpreter setting AKA interpreterSettingsRef
   *
   * e.g.:
   * "properties": {
   *   "ignite.config.url": {
   *     "name": "ignite.config.url",
   *     "value": "",
   *     "type": "url"
   *   },
   *   "ignite.clientMode": {
   *     "name": "ignite.clientMode",
   *     "value": true,
   *     "type": "checkbox"
   *   },
   *   "ignite.peerClassLoadingEnabled": {
   *     "name": "ignite.peerClassLoadingEnabled",
   *     "value": true,
   *     "type": "checkbox"
   *   },
   *   "ignite.jdbc.url": {
   *     "name": "ignite.jdbc.url",
   *     "value": "jdbc:ignite:cfg://default-ignite-jdbc.xml",
   *     "type": "string"
   *   },
   *   "ignite.addresses": {
   *     "name": "ignite.addresses",
   *     "value": "127.0.0.1:47500..47509",
   *     "type": "textarea"
   *   }
   * }
   */
  /**
   * TODO(egorklimov): очень странная логи преврашения InterpreterProperty в
   * InterpreterPropertyOld {@link InterpreterSettingRepository#convertInterpreterProperties(Object)}
   */
  private Object properties;

  private Status status;
  private String errorReason;

  @SerializedName("interpreterGroup")
  private final List<Object> interpreterInfos;

  //TODO(egorklimov): always empty?
  private final List<Dependency> dependencies;
  private InterpreterOption option;


  public InterpreterSettingV2() {
    this.id = IdHashes.generateId();
    dependencies = new ArrayList<>();
    properties = new HashMap<>();
    interpreterInfos = new ArrayList<>();
    option = new InterpreterOption();
  }

  //TODO(egorklimov) download dependencies?
  public InterpreterSettingV2(String id, String name, String group, Object properties,
      List<Object> interpreterInfos, List<Dependency> dependencies,
      InterpreterOption option) {
    this.id = id;
    this.name = name;
    this.group = group;
    this.interpreterInfos = interpreterInfos;
    this.dependencies = dependencies;
    this.option = option;
    this.status = Status.READY;

    this.properties = new HashMap<>();
    setProperties(properties);
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getGroup() {
    return group;
  }

  public Object getProperties() {
    return properties;
  }

  /**
   * To update dependencies use {@link InterpreterSettingV2#setDependencies(List)}
   * @return immutable list of dependencies
   */
  public List<Dependency> getDependencies() {
    return Collections.unmodifiableList(dependencies);
  }

  public List<Object> getInterpreterInfos() {
    return interpreterInfos;
  }

  public void setProperties(final Object object) {
    if (object instanceof Properties) {
      Properties map = (Properties) properties;
      Properties newProperties = new Properties();
      for (String key : map.stringPropertyNames()) {
        newProperties.put(key, map.get(key));
      }
      this.properties = newProperties;
    } else {
      this.properties = object;
    }
  }

  @VisibleForTesting
  public void setProperty(final String name, final String value) {
    ((Map<String, InterpreterPropertyOld>) properties).put(name, new InterpreterPropertyOld(name, value));
  }

  //TODO(egorklimov): should loadInterpreterDependencies() after updating
  public void setDependencies(final List<Dependency> dependencies) {
    this.dependencies.clear();
    this.dependencies.addAll(dependencies);
  }

  public InterpreterOption getOption() {
    return option;
  }

  public void setOption(final InterpreterOption option) {
    this.option = option;
  }

  void setGroup(final String group) {
    this.group = group;
  }

  void setName(final String name) {
    this.name = name;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public String getErrorReason() {
    return errorReason;
  }

  public void setErrorReason(String errorReason) {
    this.errorReason = errorReason;
  }

  public void setInterpreterInfos(List<Object> interpreterInfos) {
    this.interpreterInfos.clear();
    this.interpreterInfos.addAll(interpreterInfos);
  }


  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("name", name)
        .append("group", group)
        .append("properties", properties)
        .append("status", status)
        .append("errorReason", errorReason)
        .append("interpreterInfos", interpreterInfos)
        .append("dependencies", dependencies)
        .append("option", option)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InterpreterSettingV2 that = (InterpreterSettingV2) o;

    return new EqualsBuilder()
        .append(id, that.id)
        .append(name, that.name)
        .append(group, that.group)
        .append(properties, that.properties)
        .append(status, that.status)
        .append(errorReason, that.errorReason)
        .append(interpreterInfos, that.interpreterInfos)
        .append(dependencies, that.dependencies)
        .append(option, that.option)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(id)
        .append(name)
        .append(group)
        .append(properties)
        .append(status)
        .append(errorReason)
        .append(interpreterInfos)
        .append(dependencies)
        .append(option)
        .toHashCode();
  }
}
