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

package org.apache.zeppelin.notebook.display;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.zeppelin.notebook.display.ui.*;

import java.io.Serializable;
import java.util.*;


/**
 * Settings of a form.
 */
public class GUI implements Serializable {

  private static Gson gson = new GsonBuilder()
      .registerTypeAdapterFactory(Input.TypeAdapterFactory)
      .create();

  Map<String, Object> params = new HashMap<>(); // form parameters from client
  Map<String, Input> forms = new LinkedHashMap<>(); // form configuration

  public GUI() {

  }

  public void setParams(Map<String, Object> values) {
    this.params = values;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public Map<String, Input> getForms() {
    return forms;
  }

  public void setForms(Map<String, Input> forms) {
    this.forms = forms;
  }

  @Deprecated
  public Object input(String id) {
    return textbox(id, "");
  }

  @Deprecated
  public Object input(String id, Object defaultValue) {
    return textbox(id, defaultValue.toString());
  }

  public Object textbox(String id, String defaultValue) {
    // first find values from client and then use default
    Object value = params.get(id);
    if (value == null) {
      value = defaultValue;
    }

    forms.put(id, new TextBox(id, defaultValue));
    return value;
  }

  public Object textbox(String id) {
    return textbox(id, "");
  }

  public Object password(String id) {
    forms.put(id, new Password(id));
    return params.get(id);
  }

  public Object select(String id, Object defaultValue, OptionInput.ParamOption[] options) {
    if (defaultValue == null && options != null && options.length > 0) {
      defaultValue = options[0].getValue();
    }
    forms.put(id, new Select(id, defaultValue, options));
    Object value = params.get(id);
    if (value == null) {
      value = defaultValue;
      params.put(id, value);
    }
    return value;
  }

  public List<Object> checkbox(String id, Collection<Object> defaultChecked,
                               OptionInput.ParamOption[] options) {
    Collection<Object> checked = (Collection<Object>) params.get(id);
    if (checked == null) {
      checked = defaultChecked;
    }
    forms.put(id, new CheckBox(id, defaultChecked, options));
    List<Object> filtered = new LinkedList<>();
    for (Object o : checked) {
      if (isValidOption(o, options)) {
        filtered.add(o);
      }
    }
    return filtered;
  }

  private boolean isValidOption(Object o, OptionInput.ParamOption[] options) {
    for (OptionInput.ParamOption option : options) {
      if (o.equals(option.getValue())) {
        return true;
      }
    }
    return false;
  }

  public void clear() {
    this.forms = new LinkedHashMap<>();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GUI gui = (GUI) o;

    if (params != null ? !params.equals(gui.params) : gui.params != null) {
      return false;
    }
    return forms != null ? forms.equals(gui.forms) : gui.forms == null;

  }

  @Override
  public int hashCode() {
    int result = params != null ? params.hashCode() : 0;
    result = 31 * result + (forms != null ? forms.hashCode() : 0);
    return result;
  }

  public String toJson() {
    return gson.toJson(this);
  }


  public static GUI fromJson(String json) {
    GUI gui = gson.fromJson(json, GUI.class);
    //gui.convertOldInput();
    return gui;
  }
}
