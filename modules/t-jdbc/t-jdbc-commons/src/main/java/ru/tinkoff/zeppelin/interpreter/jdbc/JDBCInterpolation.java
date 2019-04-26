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

package ru.tinkoff.zeppelin.interpreter.jdbc;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

public class JDBCInterpolation {

  /**
   * Replaces all environment variables in query.
   *
   * @param query, initial query, never {@code null}.
   * @param intpContext, interpreter context, never {@code null}.
   * @return new query with replaced env variables, never {@code null}.
   */
  @Nonnull
  public static String interpolate(@Nonnull final String query,
                                   @Nonnull final Map<String, String> intpContext,
                                   @Nonnull final Set<String> envVariables) {

    final StringBuilder interpolatedPrecode = new StringBuilder(query);
    envVariables.forEach(env -> {
      while (interpolatedPrecode.indexOf(env) != -1) {
        interpolatedPrecode.replace(
            interpolatedPrecode.indexOf(env),
            interpolatedPrecode.indexOf(env) + env.length(),
            intpContext.get(env)
        );
      }
        }
    );
    return interpolatedPrecode.toString();
  }

}
