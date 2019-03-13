package org.apache.zeppelin.interpreterV2.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Represent default interpreter setting.
 */
public class BaseInterpreterConfig {

  private String group;
  private String name;
  private String className;
  private Map<String, InterpreterProperty> properties;
  private Map<String, Object> editor;


  public BaseInterpreterConfig(String name, String group, String className,
                               Map<String, InterpreterProperty> properties) {
    this.name = name;
    this.group = group;
    this.className = className;
    this.properties = properties;
    this.editor = new HashMap<>();
  }

  public String getName() {
    return name;
  }

  public String getGroup() {
    return group;
  }

  public String getClassName() {
    return className;
  }

  public Map<String, InterpreterProperty> getProperties() {
    return properties;
  }

  public Map<String, Object> getEditor() {
    return editor;
  }

  public String getInterpreterKey() {
    return getGroup() + "." + getName();
  }

}