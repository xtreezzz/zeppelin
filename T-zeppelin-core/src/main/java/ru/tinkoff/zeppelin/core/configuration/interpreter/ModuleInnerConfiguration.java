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

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Map;
import java.util.StringJoiner;
import javax.annotation.Nonnull;

/**
 * Represents default interpreter setting.
 */
public class ModuleInnerConfiguration implements Serializable {

  private long id;

  private final String className;

  private final Map<String, InterpreterProperty> properties;

  private final Map<String, Object> editor;


  public ModuleInnerConfiguration(
          final long id,
          final String className,
          final Map<String, InterpreterProperty> properties,
          final Map<String, Object> editor) {
    Preconditions.checkNotNull(className);
    Preconditions.checkNotNull(properties);
    Preconditions.checkNotNull(editor);

    this.id = id;
    this.className = className;
    this.properties = properties;
    this.editor = editor;
  }

  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  @Nonnull
  public String getClassName() {
    Preconditions.checkNotNull(className);
    return className;
  }

  @Nonnull
  public Map<String, InterpreterProperty> getProperties() {
    Preconditions.checkNotNull(properties);
    return properties;
  }

  @Nonnull
  public Map<String, Object> getEditor() {
    Preconditions.checkNotNull(editor);
    return editor;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
            .add("className='" + className + "'")
            .add("properties=" + properties)
            .add("editor=" + editor)
            .toString();
  }
}