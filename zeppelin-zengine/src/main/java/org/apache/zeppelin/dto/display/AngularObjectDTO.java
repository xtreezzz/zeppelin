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

import com.google.gson.Gson;
import java.io.Serializable;
import org.apache.zeppelin.display.AngularObject;

/**
 * Immutable Note Data Transfer Object for {@link AngularObject}
 *
 * @see org.apache.zeppelin.dto.notebook.ParagraphDTO
 */
public final class AngularObjectDTO<T> implements Serializable {
  private static final Gson gson = new Gson();

  private final String name;
  private final T object;
  private final String noteId;   // noteId belonging to. null for global scope
  private final String paragraphId; // paragraphId belongs to. null for notebook scope

  /**
   * @param name name of object
   * @param o reference to user provided object to sent to front-end
   * @param noteId noteId belongs to. can be null
   * @param paragraphId paragraphId belongs to. can be null
   */
  public AngularObjectDTO(String name, T o, String noteId, String paragraphId) {
    this.name = name;
    this.noteId = noteId;
    this.paragraphId = paragraphId;
    object = o;
  }

  // JSON conversion
  public String toJson() {
    return gson.toJson(this);
  }

  public static AngularObjectDTO fromJson(String json) {
    return gson.fromJson(json, AngularObjectDTO.class);
  }

  // AngularObject conversion
  public AngularObject toAngularObject() {
    return convertFromDtoToAngularObject(this);
  }

  public static AngularObjectDTO convertFromObjectToDTO(AngularObject source) {
    return fromJson(source.toJson());
  }

  public static AngularObject convertFromDtoToAngularObject(AngularObjectDTO source) {
    return AngularObject.fromJson(source.toJson());
  }

  // Getters
  public String getName() {
    return name;
  }

  public T getObject() {
    return object;
  }

  public String getNoteId() {
    return noteId;
  }

  public String getParagraphId() {
    return paragraphId;
  }
}
