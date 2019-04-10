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
package ru.tinkoff.zeppelin.interpreter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Interpreter {

  private Pattern envVariables = Pattern.compile("(?=(" + "Z_ENV_[a-zA-Z0-9]+" + "))");

  protected Map<String, String> configuration;
  protected String classPath;

  // service methods and fields
  private volatile String sessionUUID;
  public String getSessionUUID() {
    return sessionUUID;
  }
  public void setSessionUUID(final String sessionUUID) {
    this.sessionUUID = sessionUUID;
  }


  public Interpreter() { }

  public abstract boolean isAlive();

  public abstract boolean isOpened();

  public abstract void open(final Map<String, String> configuration, final String classPath);

  public abstract boolean isReusableForConfiguration(final Map<String, String> configuration);

  public abstract void  cancel();

  public abstract void close();

  public abstract InterpreterResult interpretV2(final String st,
                                                final Map<String, String> noteContext,
                                                final Map<String, String> userContext,
                                                final Map<String, String> configuration);

  protected Set<String> getAllEnvVeriables(final String st) {
    Set<String> matches = new HashSet<>();
    Matcher m = envVariables.matcher(st);
    while (m.find()) {
      matches.add(m.group(1));
    }
    return matches;
  }
}
