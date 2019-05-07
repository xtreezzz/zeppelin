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
package org.apache.zeppelin.rest.message;

import com.google.gson.Gson;

import java.security.InvalidParameterException;
import java.time.LocalDateTime;
import java.util.Map;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class ParagraphRequest {

  private static final Gson gson = new Gson();

  // use for updated paragraph
  private String title;
  private String text;
  private String shebang;
  private Integer position;
  private Map<String, Object> config;
  private Map<String, Object> formParams;

  // use only for send paragraph back to user
  private Long id;
  private LocalDateTime created;
  private LocalDateTime updated;

  public ParagraphRequest() {
  }

  public ParagraphRequest(final Paragraph paragraph) {
     title = paragraph.getTitle();
     text = paragraph.getText();
     shebang = paragraph.getShebang();
     position = paragraph.getPosition();
     config = paragraph.getConfig();
     formParams = paragraph.getFormParams();
     id = paragraph.getId();
     created = paragraph.getCreated();
     updated = paragraph.getUpdated();
  }

  public String getTitle() {
    return title;
  }

  public String getText() {
    return text;
  }

  public Integer getPosition() {
    return position;
  }

  public String getShebang() {
    return shebang;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public Map<String, Object> getFormParams() {
    return formParams;
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public static ParagraphRequest fromJson(final String json) {
    ParagraphRequest paragraph = gson.fromJson(json, ParagraphRequest.class);
    paragraph.checkParagraph();
    return paragraph;
  }

  private void checkParagraph() {
    //check config
    if (config != null) {

      // check config.colWidth
      if (config.get("colWidth") != null) {
        double colWidth = (double) config.get("colWidth");
        if (colWidth < 1 || colWidth > 12) {
          throw new InvalidParameterException(
              "Paragraph 'config.colWidth' it should be in [1..12] current: " + colWidth);
        }
      }

      // check config.fontSize
      if (config.get("fontSize") != null) {
        double fontSize = (double) config.get("fontSize");
        if (fontSize < 9 || fontSize > 20) {
          throw new InvalidParameterException(
              "Paragraph 'config.fontSize' it should be in [9..20] current: " + fontSize);
        }
      }
    }
  }
}
