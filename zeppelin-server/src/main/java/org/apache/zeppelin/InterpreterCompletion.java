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
package org.apache.zeppelin;

import java.util.StringJoiner;
import javax.annotation.Nonnull;

public class InterpreterCompletion implements Comparable<InterpreterCompletion> {

  @Nonnull
  private final String name;

  @Nonnull
  private final String value;

  @Nonnull
  private final String meta;

  @Nonnull
  private final String description;

  public InterpreterCompletion(@Nonnull final String name,
                               @Nonnull final String value,
                               @Nonnull final String meta,
                               @Nonnull final String description) {
    this.name = name;
    this.value = value;
    this.meta = meta;
    this.description = description;
  }

  @Nonnull
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("name='" + name + "'")
        .add("value='" + value + "'")
        .add("meta='" + meta + "'")
        .add("description='" + description + "'")
        .toString();
  }

  @Override
  public int compareTo(final InterpreterCompletion interpreterCompletion) {
    return name.compareTo(interpreterCompletion.getName());
  }
}
