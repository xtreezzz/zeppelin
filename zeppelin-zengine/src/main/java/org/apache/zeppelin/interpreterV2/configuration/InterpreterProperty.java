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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Property for registered interpreter
 */
public class InterpreterProperty implements Serializable {

  /**
   * Types of interpreter properties
   */
  private enum Type {

    TEXTAREA("textarea"),
    STRING("string"),
    NUMBER("number"),
    URL("url"),
    PASSWORD("password"),
    CHECKBOX("checkbox");

    private String value;

    Type(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private String envName;
  private String propertyName;
  private Object defaultValue;
  private String description;
  private String type;


  public InterpreterProperty(String envName, String propertyName, Object defaultValue,
                             String description, String type) {
    this.envName = envName;
    this.propertyName = propertyName;
    this.defaultValue = defaultValue;
    this.description = description;
    this.type = type;
  }

  public InterpreterProperty(Object defaultValue, String description, String type) {
    this(null, null, defaultValue, description, type);
  }

  public InterpreterProperty(Object defaultValue, String description) {
    this(null, null, defaultValue, description, Type.TEXTAREA.getValue());
  }

  public InterpreterProperty(String envName, String propertyName, String defaultValue) {
    this(envName, propertyName, defaultValue, null, Type.TEXTAREA.getValue());
  }

  public InterpreterProperty(String envName, String propertyName, String defaultValue,
                             String description) {
    this(envName, propertyName, defaultValue, description, Type.TEXTAREA.getValue());
  }

  public String getEnvName() {
    return envName;
  }

  public void setEnvName(String envName) {
    this.envName = envName;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(Object defaultValue) {
    this.defaultValue = defaultValue;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public int hashCode() {
    return this.toString().hashCode();
  }

  public boolean equals(Object o) {
    if (o == null) return false;
    return this.toString().equals(o.toString());
  }

  public static List<String> getTypes() {
    return Arrays.stream(Type.values()).map(Type::getValue).collect(Collectors.toList());
  }

  public Object getValue() {
    if (envName != null && !envName.isEmpty()) {
      String envValue = System.getenv().get(envName);
      if (envValue != null) {
        return envValue;
      }
    }

    if (propertyName != null && !propertyName.isEmpty()) {
      String propValue = System.getProperty(propertyName);
      if (propValue != null) {
        return propValue;
      }
    }
    return defaultValue;
  }

  @Override
  public String toString() {
    return String.format("{envName=%s, propertyName=%s, defaultValue=%s, description=%20s, " +
            "type=%s}", envName, propertyName, defaultValue, description, type);
  }
}
