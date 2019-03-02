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

package org.apache.zeppelin.notebook.conf;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;


/**
 * ParagraphJob runtime info based on paragraph text.
 */
public class ParagraphJobContext {

  private static final Pattern REPL_PATTERN =
      Pattern.compile("(\\s*)%([\\w\\.]+)(\\(.*?\\))?.*", Pattern.DOTALL);

  /**
   * Interpreter name.
   *
   * example: %spark.pyspark
   */
  private String interpreterShebang;

  /**
   * Paragraph source text without interpreter info.
   */
  private String scriptText;

  /**
   * Used for running selected text feature.
   */
  private String selectedText;

  /**
   * Interpreter local properties.
   *
   * https://zeppelin.apache.org/docs/0.8.0/interpreter/mahout.html#passing-a-variable-from-mahout-to-r-and-plotting
   * example: %spark.r {"imageWidth": "400px"}
   */
  private Map<String, String> localProperties;

  public ParagraphJobContext(final String paragraphText) {
    localProperties = new HashMap<>();
    updateContext(paragraphText);
  }

  public void updateContext(final String paragraphText) {
    // parse text to get interpreter component
    if (paragraphText != null) {
      // clean localProperties, otherwise previous localProperties will be used for the next run
      this.localProperties.clear();
      Matcher matcher = REPL_PATTERN.matcher(paragraphText);
      if (matcher.matches()) {
        String headingSpace = matcher.group(1);
        this.interpreterShebang = matcher.group(2);
        if (matcher.groupCount() == 3 && matcher.group(3) != null) {
          String localPropertiesText = matcher.group(3);
          String[] splits = localPropertiesText.substring(1, localPropertiesText.length() -1)
              .split(",");
          for (String split : splits) {
            String[] kv = split.split("=");
            if (StringUtils.isBlank(split) || kv.length == 0) {
              continue;
            }
            if (kv.length > 2) {
              throw new RuntimeException("Invalid paragraph properties format: " + split);
            }
            if (kv.length == 1) {
              localProperties.put(kv[0].trim(), kv[0].trim());
            } else {
              localProperties.put(kv[0].trim(), kv[1].trim());
            }
          }
          this.scriptText = paragraphText.substring(headingSpace.length() + interpreterShebang.length() +
              localPropertiesText.length() + 1).trim();
        } else {
          this.scriptText = paragraphText.substring(headingSpace.length() + interpreterShebang.length() + 1).trim();
        }
      } else {
        this.interpreterShebang = "";
        this.scriptText = paragraphText.trim();
      }
    }
  }

  public void setSelectedText(final String text) {
    this.selectedText = null;
    if (text != null && !text.isEmpty()) {
      Matcher matcher = REPL_PATTERN.matcher(text);
      int skipCount = 0;
      if (matcher.matches()) {
        skipCount += 1;
        for (int i = 1; i <= matcher.groupCount(); i++) {
          skipCount += matcher.group(i) == null ? 0 : matcher.group(i).length();
        }
      }
      this.selectedText = text.substring(skipCount).trim();
    }
  }

  public String getInterpreterShebang() {
    return interpreterShebang;
  }

  public String getScriptText() {
    return scriptText;
  }

  public String getSelectedText() {
    return selectedText;
  }

  public Map<String, String> getLocalProperties() {
    return localProperties;
  }
}
