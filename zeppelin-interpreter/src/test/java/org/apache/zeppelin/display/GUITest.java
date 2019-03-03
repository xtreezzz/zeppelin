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

package org.apache.zeppelin.display;

import org.apache.zeppelin.notebook.display.GUI;
import org.apache.zeppelin.notebook.display.ui.OptionInput;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GUITest {

  private OptionInput.ParamOption[] options = new OptionInput.ParamOption[]{
      new OptionInput.ParamOption("1", "value_1"),
      new OptionInput.ParamOption("2", "value_2")
  };

  private List<Object> checkedItems;

  @Before
  public void setUp() {
    checkedItems = new ArrayList<>();
    checkedItems.add("1");
  }

  @Test
  public void testSelect() {
    GUI gui = new GUI();
    Object selected = gui.select("list_1", null, options);
    // use the first one as the default value
    assertEquals("1", selected);

    gui = new GUI();
    selected = gui.select("list_1", "2", options);
    assertEquals("2", selected);
    // "2" is selected by above statement, so even this default value is "1", the selected value is
    // still "2"
    selected = gui.select("list_1", "1", options);
    assertEquals("2", selected);
  }

  @Test
  public void testGson() {
    GUI gui = new GUI();
    gui.textbox("textbox_1", "default_text_1");
    gui.select("select_1", "1", options);
    List<Object> list = new ArrayList();
    list.add("1");
    gui.checkbox("checkbox_1", list, options);

    String json = gui.toJson();
    System.out.println(json);
    GUI gui2 = GUI.fromJson(json);
    assertEquals(gui2.toJson(), json);
    assertEquals(gui2.getForms(), gui2.getForms());
    assertEquals(gui2.getParams(), gui2.getParams());
  }
}
