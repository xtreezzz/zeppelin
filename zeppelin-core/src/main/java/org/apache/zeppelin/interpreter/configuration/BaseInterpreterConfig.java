package org.apache.zeppelin.interpreter.configuration;

import java.io.Serializable;
import java.util.Map;
import java.util.StringJoiner;
import javax.annotation.Nonnull;

/**
 * Represents default interpreter setting.
 */
public class BaseInterpreterConfig implements Serializable {

  @Nonnull
  private final String group;
  @Nonnull
  private final String name;
  @Nonnull
  private final String className;
  @Nonnull
  private final Map<String, InterpreterProperty> properties;
  @Nonnull
  private final Map<String, Object> editor;


  public BaseInterpreterConfig(@Nonnull final String name, @Nonnull final String group,
      @Nonnull final String className, @Nonnull final Map<String, InterpreterProperty> properties,
      @Nonnull final Map<String, Object> editor) {
    this.name = name;
    this.group = group;
    this.className = className;
    this.properties = properties;
    this.editor = editor;
  }

  @Nonnull
  public String getName() {
    return name;
  }

  @Nonnull
  public String getGroup() {
    return group;
  }

  @Nonnull
  public String getClassName() {
    return className;
  }

  @Nonnull
  public Map<String, InterpreterProperty> getProperties() {
    return properties;
  }

  @Nonnull
  public Map<String, Object> getEditor() {
    return editor;
  }

  public String getInterpreterKey() {
    return getGroup() + "." + getName();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("group='" + group + "'")
        .add("name='" + name + "'")
        .add("className='" + className + "'")
        .add("properties=" + properties)
        .add("editor=" + editor)
        .toString();
  }
}