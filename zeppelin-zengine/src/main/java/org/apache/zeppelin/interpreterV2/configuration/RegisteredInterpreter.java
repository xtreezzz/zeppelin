package org.apache.zeppelin.interpreterV2.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Represent registered interpreter class
 */
public class RegisteredInterpreter {

  private String group;
  private String name;
  private String className;
  private boolean defaultInterpreter;
  private Map<String, DefaultInterpreterProperty> properties;
  private Map<String, Object> editor;
  private String path;
  private InterpreterOption option;

  public RegisteredInterpreter(String name, String group, String className,
                               Map<String, DefaultInterpreterProperty> properties) {
    this(name, group, className, false, properties);
  }

  public RegisteredInterpreter(String name, String group, String className,
                               boolean defaultInterpreter, Map<String, DefaultInterpreterProperty> properties) {
    super();
    this.name = name;
    this.group = group;
    this.className = className;
    this.defaultInterpreter = defaultInterpreter;
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

  public boolean isDefaultInterpreter() {
    return defaultInterpreter;
  }

  public void setDefaultInterpreter(boolean defaultInterpreter) {
    this.defaultInterpreter = defaultInterpreter;
  }

  public Map<String, DefaultInterpreterProperty> getProperties() {
    return properties;
  }

  public Map<String, Object> getEditor() {
    return editor;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  public String getInterpreterKey() {
    return getGroup() + "." + getName();
  }

  public InterpreterOption getOption() {
    return option;
  }
}