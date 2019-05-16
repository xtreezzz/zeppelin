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

package ru.tinkoff.zeppelin.core.configuration.interpreter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Property for registered interpreter
 */
public class ModuleProperty implements Serializable {

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

    private final String value;

    Type(final String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  @Nullable
  private String envName;

  @Nonnull
  private String propertyName;

  @Nonnull
  private Object defaultValue;

  @Nullable
  private Object currentValue;

  @Nonnull
  private String description;
  @Nonnull
  private String type;

  public ModuleProperty(@Nonnull final String envName, @Nonnull final String propertyName,
                        @Nonnull final Object defaultValue, @Nonnull final Object currentValue,
                        @Nonnull final String description, @Nonnull final String type) {
    this.envName = envName;
    this.propertyName = propertyName;
    this.defaultValue = defaultValue;
    this.currentValue = currentValue;
    this.description = description;
    this.type = type;
  }

  public ModuleProperty(@Nonnull final String envName, @Nonnull final String propertyName,
                        @Nonnull final Object defaultValue, @Nonnull final String description,
                        @Nonnull final String type) {
    this.envName = envName;
    this.propertyName = propertyName;
    this.defaultValue = defaultValue;
    this.description = description;
    this.type = type;
  }

  public ModuleProperty(@Nonnull final String envName, @Nonnull final String propertyName,
                        @Nonnull final String defaultValue, @Nonnull final String description) {
    this(envName, propertyName, defaultValue, description, Type.TEXTAREA.getValue());
  }

  @Nullable
  public Object getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(@Nonnull final Object currentValue) {
    this.currentValue = currentValue;
  }

  @Nonnull
  public String getEnvName() {
    return envName;
  }

  public void setEnvName(@Nonnull final String envName) {
    this.envName = envName;
  }

  @Nonnull
  public String getPropertyName() {
    return propertyName;
  }

  public void setPropertyName(@Nonnull final String propertyName) {
    this.propertyName = propertyName;
  }

  @Nonnull
  public Object getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(@Nonnull final Object defaultValue) {
    this.defaultValue = defaultValue;
  }

  @Nonnull
  public String getDescription() {
    return description;
  }

  public void setDescription(@Nonnull final String description) {
    this.description = description;
  }

  @Nonnull
  public String getType() {
    return type;
  }

  public void setType(@Nonnull final String type) {
    // Check that type is correct
    if (getTypes().contains(type)) {
      this.type = type;
    }
  }

  @Nonnull
  public static List<String> getTypes() {
    return Arrays.stream(Type.values()).map(Type::getValue).collect(Collectors.toList());
  }

  @Nonnull
  public Object getValue() {
    if (!envName.isEmpty()) {
      final String envValue = System.getenv().get(envName);
      if (envValue != null) {
        return envValue;
      }
    }

    if (!propertyName.isEmpty()) {
      final String propValue = System.getProperty(propertyName);
      if (propValue != null) {
        return propValue;
      }
    }

    if (currentValue == null) {
      return defaultValue;
    }

    return currentValue;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ModuleProperty that = (ModuleProperty) o;

    return new EqualsBuilder()
        .append(envName, that.envName)
        .append(propertyName, that.propertyName)
        .append(defaultValue, that.defaultValue)
        .append(currentValue, that.currentValue)
        .append(description, that.description)
        .append(type, that.type)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(envName)
        .append(propertyName)
        .append(defaultValue)
        .append(currentValue)
        .append(description)
        .append(type)
        .toHashCode();
  }

  @Override
  public String toString() {
    return String.format("{envName=%s, propertyName=%s, currentValue=%s, defaultValue=%s, description=%20s, " +
        "type=%s}", envName, propertyName, currentValue, defaultValue, description, type);
  }
}
