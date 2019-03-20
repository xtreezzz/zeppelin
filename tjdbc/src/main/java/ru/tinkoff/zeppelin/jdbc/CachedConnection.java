/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package ru.tinkoff.zeppelin.jdbc;

import com.google.gson.Gson;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedConnection implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(CachedConnection.class);
  private static final Gson gson = new Gson();

  private static final String META_DIR = "Metadata/";

  private Connection connection;
  private Supplier<Connection> connectionSupplier;
  private String url;

  public CachedConnection(final Supplier<Connection> connectionSupplier, final String url) {
    this.connectionSupplier = connectionSupplier;
    this.url = url;
  }

  @Override
  public void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ignore) {
      }
    }
  }

  private Connection getConnection() {
    if (connection == null) {
      connection = connectionSupplier.get();
    }
    return connection;
  }

  String getSchema() throws SQLException {
    return getValue("getSchema", getConnection()::getSchema, String.class);
  }

  String getCatalog() throws SQLException {
    return getValue("getCatalog", getConnection()::getCatalog, String.class);
  }

  String getUserName() throws SQLException {
    return getValue("getUserName", getConnection().getMetaData()::getUserName, String.class);
  }

  List<Map<String, Object>> getSchemas() throws SQLException {
    return getValue("getSchemas", () -> resultSetToMap(getConnection().getMetaData().getSchemas()),
        String.class);
  }

  List<Map<String, Object>> getCatalogs() throws SQLException {
    return getValue("getCatalogs",
        () -> resultSetToMap(getConnection().getMetaData().getCatalogs()), String.class);
  }

  List<Map<String, Object>> getTables(String schema, String schema1, String s, String[] strings)
      throws SQLException {
    return getValue(
        schema + "." + schema1 + "." + s + "." + Arrays.stream(strings)
            .collect(Collectors.joining(".")),
        () -> resultSetToMap(getConnection().getMetaData().getTables(schema, schema1, s, strings)),
        ResultSet.class);
  }

  List<Map<String, Object>> getColumns(String schema, String schema1, String table, String s)
      throws SQLException {
    return getValue(
        schema + "." + schema1 + "." + s,
        () -> resultSetToMap(getConnection().getMetaData().getColumns(schema, schema1, table, s)),
        ResultSet.class);
  }

  private List<Map<String, Object>> resultSetToMap(ResultSet resultSet) {
    try {
      List<Map<String, Object>> list = new ArrayList<>();
      while (resultSet.next()) {
        Map<String, Object> map = new HashMap<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          String columnName = metaData.getColumnName(i);
          map.put(columnName.toUpperCase(), resultSet.getString(columnName));
        }
        list.add(map);
      }
      return list;
    } catch (Exception e) {
      log.error("Can't convert resultSetToMap", e);
    }
    return null;
  }

  private <T> T getValue(String key, SupplierWithException<T> valueSupplier, Class clazz) {
    try {
      int hash = (url + key).hashCode();
      final File metaFolder = new File(META_DIR);
      if (!metaFolder.exists()) {
        metaFolder.mkdir();
      }
      File file = new File(META_DIR + hash + ".meta");
      if (file.exists()) {
        return (T) gson.fromJson(new String(Files.readAllBytes(file.toPath())), clazz);
      } else {
        Files.createFile(file.toPath());
      }
      T value = valueSupplier.get();
      Files.write(file.toPath(), gson.toJson(value).getBytes(), StandardOpenOption.WRITE);
      return value;
    } catch (Exception e) {
      log.error("Error then trying get cached value", e);
      return null;
    }
  }

//  private <T> T getValue(String key, SupplierWithException<T> valueSupplier, Class clazz) {
//    try {
//      int hash = (url + key).hashCode();
//      final File metaFolder = new File(META_DIR);
//      if(!metaFolder.exists()) {
//        metaFolder.mkdir();
//      }
//      ObjectMapper mapper = new ObjectMapper();
//      File file = new File(META_DIR + hash + ".meta");
//      if (file.exists()) {
//        return (T) mapper.readValue(file, clazz);
//      }
//      T value = valueSupplier.get();
//      mapper.writeValue(file, value);
//      return value;
//    } catch (Exception e) {
//      log.error("Error then trying get cached value", e);
//      return null;
//    }
//  }
}
