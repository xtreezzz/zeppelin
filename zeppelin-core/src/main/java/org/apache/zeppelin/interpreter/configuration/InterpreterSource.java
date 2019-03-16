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

package org.apache.zeppelin.interpreter.configuration;

import com.google.gson.Gson;
import java.io.Serializable;
import java.util.StringJoiner;

public class InterpreterSource implements Serializable {

  private String interpreterName;
  private String artifact;

  public InterpreterSource(String interpreterName, String artifact) {
    this.interpreterName = interpreterName;
    this.artifact = artifact;
  }

  public String getInterpreterName() {
    return interpreterName;
  }

  public void setInterpreterName(String interpreterName) {
    this.interpreterName = interpreterName;
  }

  public String getArtifact() {
    return artifact;
  }

  public void setArtifact(String artifact) {
    this.artifact = artifact;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("interpreterName='" + interpreterName + "'")
        .add("artifact='" + artifact + "'")
        .toString();
  }

  public static InterpreterSource fromJson(final String json) {
    return new Gson().fromJson(json, InterpreterSource.class);
  }
}
