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
package ru.tinkoff.zeppelin.jdbc.enums;

public enum InterpreterProperties {
  INTERPRETER_NAME("tjdbc"),
  COMMON_KEY("common"),
  MAX_LINE_KEY("max_count"),
  MAX_LINE_DEFAULT("1000"),

  DEFAULT_KEY("default"),
  DRIVER_KEY("driver"),
  URL_KEY("url"),
  USER_KEY("user"),
  PASSWORD_KEY("password"),
  PRECODE_KEY("precode"),
  STATEMENT_PRECODE_KEY("statementPrecode"),
  COMPLETER_SCHEMA_FILTERS_KEY("completer.schemaFilters"),
  COMPLETER_TTL_KEY("completer.ttlInSeconds"),
  DEFAULT_COMPLETER_TTL("120"),
  SPLIT_QURIES_KEY("splitQueries"),
  JDBC_JCEKS_FILE("jceks.file"),
  JDBC_JCEKS_CREDENTIAL_KEY("jceks.credentialKey"),
  PRECODE_KEY_TEMPLATE("%s.precode"),
  STATEMENT_PRECODE_KEY_TEMPLATE("%s.statementPrecode"),
  DOT("."),

  WHITESPACE(" "),
  NEWLINE("\n"),
  TAB("\t"),
  TABLE_MAGIC_TAG("%table "),
  EXPLAIN_PREDICATE("EXPLAIN "),

  COMMON_MAX_LINE(COMMON_KEY.getValue() + DOT.getValue() + MAX_LINE_KEY.getValue()),
  DEFAULT_DRIVER(DEFAULT_KEY.getValue() + DOT.getValue() + DRIVER_KEY.getValue()),
  DEFAULT_URL(DEFAULT_KEY.getValue() + DOT.getValue() + URL_KEY.getValue()),
  DEFAULT_USER(DEFAULT_KEY.getValue() + DOT.getValue() + USER_KEY.getValue()),
  DEFAULT_PASSWORD(DEFAULT_KEY.getValue() + DOT.getValue() + PASSWORD_KEY.getValue()),
  DEFAULT_PRECODE(DEFAULT_KEY.getValue() + DOT.getValue() + PRECODE_KEY.getValue()),
  DEFAULT_STATEMENT_PRECODE(
      DEFAULT_KEY.getValue() + DOT.getValue() + STATEMENT_PRECODE_KEY.getValue()),

  EMPTY_COLUMN_VALUE(""),

  CONCURRENT_EXECUTION_KEY("zeppelin.jdbc.concurrent.use"),
  CONCURRENT_EXECUTION_COUNT("zeppelin.jdbc.concurrent.max_connection"),
  DBCP_STRING("jdbc:apache:commons:dbcp:"),
  MAX_ROWS_KEY("zeppelin.jdbc.maxRows");

  InterpreterProperties(String val) {
    value = val;
  }

  public String getValue() {
    return value;
  }

  private String value;
}

