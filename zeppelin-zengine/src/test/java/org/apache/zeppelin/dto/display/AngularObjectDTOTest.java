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

package org.apache.zeppelin.dto.display;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.apache.zeppelin.display.AngularObject;
import org.junit.Test;

public class AngularObjectDTOTest {

  @Test
  public void testJsonConversion() {
    assertEquals(
        new AngularObject<>("name", "value", "note1", null, null)
        .toJson(),
        new AngularObjectDTO<>("name", "value", "note1", null).toJson()
    );

    assertEquals(
        new AngularObject<>("name", "value", "note1", "paragraph1", null).toJson(),
        new AngularObjectDTO<>("name", "value", "note1", "paragraph1").toJson()
    );

    assertEquals(
        new AngularObject<>("name", "value", null, null, null).toJson(),
        new AngularObjectDTO<>("name", "value", null, null).toJson()
    );

    assertNotSame(
        new AngularObject<>("name1", "value", null, null, null).toJson(),
        new AngularObjectDTO<>("name2", "value", null, null).toJson()
    );

    assertNotSame(
        new AngularObject<>("name1", "value", "note1", null, null).toJson(),
        new AngularObjectDTO<>("name2", "value", "note2", null).toJson()
    );

    assertNotSame(
        new AngularObject<>("name1", "value", "note", null, null).toJson(),
        new AngularObjectDTO<>("name2", "value", null, null).toJson()
    );

    assertNotSame(
        new AngularObject<>("name", "value", "note", "paragraph1", null).toJson(),
        new AngularObjectDTO<>("name", "value", "note", "paragraph2").toJson()
    );

    assertNotSame(
        new AngularObject<>("name", "value", "note1", null, null).toJson(),
        new AngularObjectDTO<>("name", "value", "note1", "paragraph1").toJson()
    );
  }

  @Test
  public void testAngularObjectConversion() {

    AngularObject<String> original = new AngularObject<>(
        "name", "value", "note1", null, null);

    assertEquals(
        original,
        AngularObjectDTO.convertFromDtoToAngularObject(
            AngularObjectDTO.convertFromObjectToDTO(original)
        )
    );
  }
}