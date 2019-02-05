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

import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.COMMON_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.COMPLETER_SCHEMA_FILTERS_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.COMPLETER_TTL_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.CONCURRENT_EXECUTION_COUNT;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.CONCURRENT_EXECUTION_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.DBCP_STRING;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.DEFAULT_COMPLETER_TTL;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.DEFAULT_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.DRIVER_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.EMPTY_COLUMN_VALUE;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.EXPLAIN_PREDICATE;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.INTERPRETER_NAME;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.JDBC_JCEKS_CREDENTIAL_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.JDBC_JCEKS_FILE;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.MAX_LINE_DEFAULT;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.MAX_LINE_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.MAX_ROWS_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.NEWLINE;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.PASSWORD_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.SPLIT_QURIES_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.STATEMENT_PRECODE_KEY_TEMPLATE;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.TAB;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.TABLE_MAGIC_TAG;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.URL_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.USER_KEY;
import static ru.tinkoff.zeppelin.jdbc.enums.InterpreterProperties.WHITESPACE;

import java.util.Map.Entry;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.KerberosInterpreter;
import org.apache.zeppelin.interpreter.ResultMessages;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.user.UserCredentials;
import org.apache.zeppelin.user.UsernamePassword;

import ru.tinkoff.zeppelin.jdbc.security.JDBCSecurityImpl;


/**
 * JDBC interpreter for Zeppelin. This interpreter can also be used for accessing HAWQ,
 * GreenplumDB, MariaDB, MySQL, Postgres and Redshift.
 *
 * <ul>
 * <li>{@code default.url} - JDBC URL to connect to.</li>
 * <li>{@code default.user} - JDBC user name..</li>
 * <li>{@code default.password} - JDBC password..</li>
 * <li>{@code default.driver.name} - JDBC driver name.</li>
 * <li>{@code common.max.result} - Max number of SQL result to display.</li>
 * </ul>
 *
 * <p>
 * How to use: <br/>
 * {@code %jdbc.sql} <br/>
 * {@code
 * SELECT store_id, count(*)
 * FROM retail_demo.order_lineitems_pxf
 * GROUP BY store_id;
 * }
 * </p>
 */
public class JDBCInterpreter extends KerberosInterpreter {
  private Logger logger = LoggerFactory.getLogger(JDBCInterpreter.class);

  private final HashMap<String, Properties> basePropretiesMap;
  private final HashMap<String, JDBCUserConfigurations> jdbcUserConfigurationsMap;
  private final HashMap<String, SqlCompleter> sqlCompletersMap;

  private int maxLineResults;
  private int maxRows;

  public JDBCInterpreter(Properties property) {
    super(property);
    jdbcUserConfigurationsMap = new HashMap<>();
    basePropretiesMap = new HashMap<>();
    sqlCompletersMap = new HashMap<>();
    maxLineResults = Integer.valueOf(MAX_LINE_DEFAULT.getValue());
  }

  @Override
  protected boolean runKerberosLogin() {
    try {
      if (UserGroupInformation.isLoginKeytabBased()) {
        UserGroupInformation.getLoginUser().reloginFromKeytab();
        return true;
      } else if (UserGroupInformation.isLoginTicketBased()) {
        UserGroupInformation.getLoginUser().reloginFromTicketCache();
        return true;
      }
    } catch (Exception e) {
      logger.error("Unable to run kinit for zeppelin", e);
    }
    return false;
  }

  public Map<String, Properties> getPropertiesMap() {
    return basePropretiesMap;
  }

  @Override
  public void open() {
    super.open();
    for (String propertyKey : properties.stringPropertyNames()) {
      logger.debug("propertyKey: {}", propertyKey);
      String[] keyValue = propertyKey.split("\\.", 2);
      if (2 == keyValue.length) {
        logger.debug("key: {}, value: {}", keyValue[0], keyValue[1]);

        Properties prefixProperties;
        if (basePropretiesMap.containsKey(keyValue[0])) {
          prefixProperties = basePropretiesMap.get(keyValue[0]);
        } else {
          prefixProperties = new Properties();
          basePropretiesMap.put(keyValue[0].trim(), prefixProperties);
        }
        prefixProperties.put(keyValue[1].trim(), getProperty(propertyKey));
      }
    }

    Set<String> removeKeySet = new HashSet<>();
    for (Entry<String, Properties> keyValuePair : basePropretiesMap.entrySet()) {
      Properties properties = keyValuePair.getValue();
      String key = keyValuePair.getKey();
      if (!COMMON_KEY.getValue().equals(key)
          && (!properties.containsKey(DRIVER_KEY.getValue())
          || !properties.containsKey(URL_KEY.getValue()))) {

        logger.error("{} will be ignored. {}.{} and {}.{} is mandatory.",
            key, DRIVER_KEY.getValue(), key, key, URL_KEY.getValue());
        removeKeySet.add(key);
      }
    }

    for (String key : removeKeySet) {
      basePropretiesMap.remove(key);
    }
    logger.debug("JDBC PropretiesMap: {}", basePropretiesMap);

    setMaxLineResults();
    setMaxRows();
  }

  protected boolean isKerboseEnabled() {
    if (!isEmpty(getProperty("zeppelin.jdbc.auth.type"))) {
      UserGroupInformation.AuthenticationMethod authType = JDBCSecurityImpl.getAuthtype(properties);
      return authType.equals(KERBEROS);
    }
    return false;
  }

  private void setMaxLineResults() {
    if (basePropretiesMap.containsKey(COMMON_KEY.getValue()) &&
        basePropretiesMap.get(COMMON_KEY.getValue()).containsKey(MAX_LINE_KEY.getValue())) {
      maxLineResults = Integer.valueOf(
          basePropretiesMap.get(COMMON_KEY.getValue()).getProperty(MAX_LINE_KEY.getValue()));
    }
  }

  /**
   * Fetch MAX_ROWS_KEYS value from property file and set it to "maxRows" value.
   */
  private void setMaxRows() {
    maxRows = Integer.valueOf(getProperty(MAX_ROWS_KEY.getValue(), "1000"));
  }

  private SqlCompleter createOrUpdateSqlCompleter(SqlCompleter sqlCompleter,
      final Connection connection, String propertyKey, final String buf, final int cursor) {
    String schemaFiltersKey = String
        .format("%s.%s", propertyKey, COMPLETER_SCHEMA_FILTERS_KEY.getValue());
    String sqlCompleterTtlKey = String.format("%s.%s", propertyKey, COMPLETER_TTL_KEY.getValue());
    final String schemaFiltersString = getProperty(schemaFiltersKey);
    int ttlInSeconds = Integer.valueOf(
        StringUtils
            .defaultIfEmpty(getProperty(sqlCompleterTtlKey), DEFAULT_COMPLETER_TTL.getValue())
    );
    final SqlCompleter completer;
    if (sqlCompleter == null) {
      completer = new SqlCompleter(ttlInSeconds);
    } else {
      completer = sqlCompleter;
    }
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    executorService.execute(() -> {
      completer.createOrUpdateFromConnection(connection, schemaFiltersString, buf, cursor);
    });

    executorService.shutdown();

    try {
      // protection to release connection
      executorService.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Completion timeout", e);
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e1) {
          logger.warn("Error close connection", e1);
        }
      }
    }
    return completer;
  }

  private void initStatementMap() {
    for (JDBCUserConfigurations configurations : jdbcUserConfigurationsMap.values()) {
      try {
        configurations.initStatementMap();
      } catch (Exception e) {
        logger.error("Error while closing paragraphIdStatementMap statement...", e);
      }
    }
  }

  private void initConnectionPoolMap() {
    for (Entry<String, JDBCUserConfigurations> keyValuePair : jdbcUserConfigurationsMap
        .entrySet()) {
      try {
        closeDBPool(keyValuePair.getKey(), DEFAULT_KEY.getValue());
      } catch (SQLException e) {
        logger.error("Error while closing database pool.", e);
      }
      keyValuePair.getValue().initConnectionPoolMap();
    }
  }

  @Override
  public void close() {
    super.close();
    try {
      initStatementMap();
      initConnectionPoolMap();
    } catch (Exception e) {
      logger.error("Error while closing...", e);
    }
  }

  private String getEntityName(String replName) {
    return String.join("", INTERPRETER_NAME.getValue(), ".", replName);
  }

  private String getJDBCDriverName(String user, String propertyKey) {
    return String.join("", DBCP_STRING.getValue(), propertyKey, user);
  }

  private boolean existAccountInBaseProperty(String propertyKey) {
    return basePropretiesMap.get(propertyKey).containsKey(USER_KEY.getValue()) &&
        !isEmpty((String) basePropretiesMap.get(propertyKey).get(USER_KEY.getValue())) &&
        basePropretiesMap.get(propertyKey).containsKey(PASSWORD_KEY.getValue());
  }

  private UsernamePassword getUsernamePassword(InterpreterContext interpreterContext,
      String replName) {
    UserCredentials uc = interpreterContext.getAuthenticationInfo().getUserCredentials();
    if (uc != null) {
      return uc.getUsernamePassword(replName);
    }
    return null;
  }

  public JDBCUserConfigurations getJDBCConfiguration(String user) {
    return jdbcUserConfigurationsMap.computeIfAbsent(user, v -> new JDBCUserConfigurations());
  }

  private void closeDBPool(String user, String propertyKey) throws SQLException {
    PoolingDriver poolingDriver = getJDBCConfiguration(user).removeDBDriverPool(propertyKey);
    if (poolingDriver != null) {
      poolingDriver.closePool(propertyKey + user);
    }
  }

  private void setUserProperty(String propertyKey, InterpreterContext interpreterContext)
      throws SQLException, IOException, InterpreterException {

    String user = interpreterContext.getAuthenticationInfo().getUser();

    JDBCUserConfigurations jdbcUserConfigurations = getJDBCConfiguration(user);
    if (basePropretiesMap.get(propertyKey).containsKey(USER_KEY.getValue()) &&
        !basePropretiesMap.get(propertyKey).getProperty(USER_KEY.getValue()).isEmpty()) {
      String password = getPassword(basePropretiesMap.get(propertyKey));
      if (!isEmpty(password)) {
        basePropretiesMap.get(propertyKey).setProperty(PASSWORD_KEY.getValue(), password);
      }
    }
    jdbcUserConfigurations.setPropertyMap(propertyKey, basePropretiesMap.get(propertyKey));
    if (existAccountInBaseProperty(propertyKey)) {
      return;
    }
    jdbcUserConfigurations.cleanUserProperty(propertyKey);

    UsernamePassword usernamePassword = getUsernamePassword(interpreterContext,
        getEntityName(interpreterContext.getReplName()));
    if (usernamePassword != null) {
      jdbcUserConfigurations.setUserProperty(propertyKey, usernamePassword);
    } else {
      closeDBPool(user, propertyKey);
    }
  }

  private void createConnectionPool(String url, String user, String propertyKey,
      Properties properties) throws ClassNotFoundException {
    ConnectionFactory connectionFactory =
        new DriverManagerConnectionFactory(url, properties);

    PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
        connectionFactory, null);
    final String maxConnectionLifetime =
        StringUtils.defaultIfEmpty(getProperty("zeppelin.jdbc.maxConnLifetime"), "-1");
    poolableConnectionFactory.setMaxConnLifetimeMillis(Long.parseLong(maxConnectionLifetime));

    ObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory);

    poolableConnectionFactory.setPool(connectionPool);
    Class.forName(properties.getProperty(DRIVER_KEY.getValue()));
    PoolingDriver driver = new PoolingDriver();
    driver.registerPool(propertyKey + user, connectionPool);
    getJDBCConfiguration(user).saveDBDriverPool(propertyKey, driver);
  }

  private Connection getConnectionFromPool(String url, String user, String propertyKey,
      Properties properties) throws SQLException, ClassNotFoundException {
    String jdbcDriver = getJDBCDriverName(user, propertyKey);

    if (!getJDBCConfiguration(user).isConnectionInDBDriverPool(propertyKey)) {
      createConnectionPool(url, user, propertyKey, properties);
    }
    return DriverManager.getConnection(jdbcDriver);
  }

  private Connection getConnection(String propertyKey, InterpreterContext interpreterContext)
      throws ClassNotFoundException, SQLException, InterpreterException, IOException {
    final String user = interpreterContext.getAuthenticationInfo().getUser();
    Connection connection;
    if (propertyKey == null || basePropretiesMap.get(propertyKey) == null) {
      return null;
    }

    JDBCUserConfigurations jdbcUserConfigurations = getJDBCConfiguration(user);
    setUserProperty(propertyKey, interpreterContext);

    final Properties properties = jdbcUserConfigurations.getPropertyMap(propertyKey);
    final String url = properties.getProperty(URL_KEY.getValue());

    if (isEmpty(getProperty("zeppelin.jdbc.auth.type"))) {
      connection = getConnectionFromPool(url, user, propertyKey, properties);
    } else {
      UserGroupInformation.AuthenticationMethod authType =
          JDBCSecurityImpl.getAuthtype(getProperties());

      final String connectionUrl = appendProxyUserToURL(url, user, propertyKey);

      JDBCSecurityImpl.createSecureConfiguration(getProperties(), authType);
      if (authType == KERBEROS) {
        if (user == null || "false".equalsIgnoreCase(
            getProperty("zeppelin.jdbc.auth.kerberos.proxy.enable"))) {
          connection = getConnectionFromPool(connectionUrl, user, propertyKey, properties);
        } else {
          if (basePropretiesMap.get(propertyKey).containsKey("proxy.user.property")) {
            connection = getConnectionFromPool(connectionUrl, user, propertyKey, properties);
          } else {
            UserGroupInformation ugi = null;
            try {
              ugi = UserGroupInformation.createProxyUser(
                  user, UserGroupInformation.getCurrentUser());
            } catch (Exception e) {
              logger.error("Error in getCurrentUser", e);
              throw new InterpreterException("Error in getCurrentUser", e);
            }

            final String poolKey = propertyKey;
            try {
              connection = ugi.doAs(
                  (PrivilegedExceptionAction<Connection>) () ->
                      getConnectionFromPool(connectionUrl, user, poolKey, properties));
            } catch (Exception e) {
              logger.error("Error in doAs", e);
              throw new InterpreterException("Error in doAs", e);
            }
          }
        }
      } else {
        connection = getConnectionFromPool(connectionUrl, user, propertyKey, properties);
      }
    }

    return connection;
  }

  private String appendProxyUserToURL(String url, String user, String propertyKey) {
    StringBuilder connectionUrl = new StringBuilder(url);

    if (user != null && !user.equals("anonymous") &&
        basePropretiesMap.get(propertyKey).containsKey("proxy.user.property")) {

      int lastIndexOfUrl = connectionUrl.indexOf("?");
      if (lastIndexOfUrl == -1) {
        lastIndexOfUrl = connectionUrl.length();
      }
      logger.info("Using proxy user as : {}", user);
      logger.info("Using proxy property for user as :" +
          basePropretiesMap.get(propertyKey).getProperty("proxy.user.property"));
      connectionUrl.insert(lastIndexOfUrl, ";" +
          basePropretiesMap.get(propertyKey).getProperty("proxy.user.property") + "=" + user + ";");
    } else if (user != null && !user.equals("anonymous") && url.contains("hive")) {
      logger.warn("User impersonation for hive has changed please refer: http://zeppelin.apache" +
          ".org/docs/latest/interpreter/jdbc.html#apache-hive");
    }

    return connectionUrl.toString();
  }

  private String getPassword(Properties properties) throws IOException, InterpreterException {
    if (isNotEmpty(properties.getProperty(PASSWORD_KEY.getValue()))) {
      return properties.getProperty(PASSWORD_KEY.getValue());
    } else if (isNotEmpty(properties.getProperty(JDBC_JCEKS_FILE.getValue()))
        && isNotEmpty(properties.getProperty(JDBC_JCEKS_CREDENTIAL_KEY.getValue()))) {
      try {
        Configuration configuration = new Configuration();
        configuration.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
            properties.getProperty(JDBC_JCEKS_FILE.getValue()));
        CredentialProvider provider = CredentialProviderFactory.getProviders(configuration).get(0);
        CredentialProvider.CredentialEntry credEntry =
            provider
                .getCredentialEntry(properties.getProperty(JDBC_JCEKS_CREDENTIAL_KEY.getValue()));
        if (credEntry != null) {
          return new String(credEntry.getCredential());
        } else {
          throw new InterpreterException("Failed to retrieve password from JCEKS from key: "
              + properties.getProperty(JDBC_JCEKS_CREDENTIAL_KEY.getValue()));
        }
      } catch (Exception e) {
        logger.error("Failed to retrieve password from JCEKS \n" +
            "For file: " + properties.getProperty(JDBC_JCEKS_FILE.getValue()) +
            "\nFor key: " + properties.getProperty(JDBC_JCEKS_CREDENTIAL_KEY.getValue()), e);
        throw e;
      }
    }
    return null;
  }

  private String getResults(ResultSet resultSet, boolean isTableType, MutableBoolean isComplete)
      throws SQLException {
    ResultSetMetaData md = resultSet.getMetaData();
    StringBuilder msg;
    if (isTableType) {
      msg = new StringBuilder(TABLE_MAGIC_TAG.getValue());
    } else {
      msg = new StringBuilder();
    }

    for (int i = 1; i < md.getColumnCount() + 1; i++) {
      if (i > 1) {
        msg.append(TAB.getValue());
      }
      if (StringUtils.isNotEmpty(md.getColumnLabel(i))) {
        msg.append(replaceReservedChars(md.getColumnLabel(i)));
      } else {
        msg.append(replaceReservedChars(md.getColumnName(i)));
      }
    }
    msg.append(NEWLINE.getValue());

    int displayRowCount = 0;
    while (resultSet.next()) {
      if (displayRowCount >= getMaxResult()) {
        isComplete.setValue(false);
        break;
      }
      for (int i = 1; i < md.getColumnCount() + 1; i++) {
        Object resultObject;
        String resultValue;
        resultObject = resultSet.getObject(i);
        if (resultObject == null) {
          resultValue = "null";
        } else {
          resultValue = resultSet.getString(i);
        }
        msg.append(replaceReservedChars(resultValue));
        if (i != md.getColumnCount()) {
          msg.append(TAB.getValue());
        }
      }
      msg.append(NEWLINE.getValue());
      displayRowCount++;
    }
    return msg.toString();
  }

  private boolean isDDLCommand(int updatedCount, int columnCount) {
    return updatedCount < 0 && columnCount <= 0;
  }

  /*
  inspired from https://github.com/postgres/pgadmin3/blob/794527d97e2e3b01399954f3b79c8e2585b908dd/
    pgadmin/dlg/dlgProperty.cpp#L999-L1045
   */
  protected ArrayList<String> splitSqlQueries(String sql) {
    ArrayList<String> queries = new ArrayList<>();
    StringBuilder query = new StringBuilder();
    char character;

    boolean multiLineComment = false;
    boolean singleLineComment = false;
    boolean quoteString = false;
    boolean doubleQuoteString = false;

    for (int item = 0; item < sql.length(); item++) {
      character = sql.charAt(item);

      if (singleLineComment && (character == '\n' || item == sql.length() - 1)) {
        singleLineComment = false;
      }

      if (multiLineComment && character == '/' && sql.charAt(item - 1) == '*') {
        multiLineComment = false;
      }

      if (character == '\'') {
        if (quoteString) {
          quoteString = false;
        } else if (!doubleQuoteString) {
          quoteString = true;
        }
      }

      if (character == '"') {
        if (doubleQuoteString && item > 0) {
          doubleQuoteString = false;
        } else if (!quoteString) {
          doubleQuoteString = true;
        }
      }

      if (!quoteString && !doubleQuoteString && !multiLineComment && !singleLineComment
          && sql.length() > item + 1) {
        if (character == '-' && sql.charAt(item + 1) == '-') {
          singleLineComment = true;
        } else if (character == '/' && sql.charAt(item + 1) == '*') {
          multiLineComment = true;
        }
      }

      if (character == ';' && !quoteString && !doubleQuoteString && !multiLineComment
          && !singleLineComment) {
        queries.add(StringUtils.trim(query.toString()));
        query = new StringBuilder();
      } else if (item == sql.length() - 1) {
        query.append(character);
        queries.add(StringUtils.trim(query.toString()));
      } else {
        query.append(character);
      }
    }

    return queries;
  }

  @Override
  public InterpreterResult executePrecode(InterpreterContext interpreterContext) {
    InterpreterResult interpreterResult = null;
    for (String propertyKey : basePropretiesMap.keySet()) {
      String precode = getProperty(String.format("%s.precode", propertyKey));
      if (StringUtils.isNotBlank(precode)) {
        interpreterResult = executeSql(propertyKey, precode, interpreterContext);
        if (interpreterResult.code() != Code.SUCCESS) {
          break;
        }
      }
    }

    return interpreterResult;
  }

  private InterpreterResult executeSql(String propertyKey, String sql,
      InterpreterContext interpreterContext) {
    Connection connection = null;
    Statement statement;
    ResultSet resultSet = null;
    String paragraphId = interpreterContext.getParagraphId();
    String user = interpreterContext.getAuthenticationInfo().getUser();

    boolean splitQuery = false;
    String splitQueryProperty = getProperty(String.format("%s.%s", propertyKey, SPLIT_QURIES_KEY));
    if (StringUtils.isNotBlank(splitQueryProperty) && splitQueryProperty.equalsIgnoreCase("true")) {
      splitQuery = true;
    }

    InterpreterResult interpreterResult = new InterpreterResult(Code.SUCCESS);
    try {
      connection = getConnection(propertyKey, interpreterContext);
    } catch (Exception e) {
      String errorMsg = ExceptionUtils.getStackTrace(e);
      try {
        closeDBPool(user, propertyKey);
      } catch (SQLException e1) {
        logger.error("Cannot close DBPool for user, propertyKey: " + user + propertyKey, e1);
      }
      interpreterResult.add(errorMsg);
      return new InterpreterResult(Code.ERROR, interpreterResult.message());
    }
    if (connection == null) {
      return new InterpreterResult(Code.ERROR, "Prefix not found.");
    }

    try {
      List<String> sqlArray;
      if (splitQuery) {
        sqlArray = splitSqlQueries(sql);
      } else {
        sqlArray = Arrays.asList(sql);
      }

      for (String sqlToExecute : sqlArray) {
        statement = connection.createStatement();

        if (statement == null) {
          return new InterpreterResult(Code.ERROR, "Cannot create statement.");
        }

        // fetch n+1 rows in order to indicate there's more rows available (for large selects)
        statement.setFetchSize(getMaxResult());
        statement.setMaxRows(maxRows);

        try {
          getJDBCConfiguration(user).saveStatement(paragraphId, statement);

          String statementPrecode =
              getProperty(String.format(STATEMENT_PRECODE_KEY_TEMPLATE.getValue(), propertyKey));

          if (StringUtils.isNotBlank(statementPrecode)) {
            statement.execute(statementPrecode);
          }

          boolean isResultSetAvailable = statement.execute(sqlToExecute);
          getJDBCConfiguration(user).setConnectionInDBDriverPoolSuccessful(propertyKey);
          if (isResultSetAvailable) {
            resultSet = statement.getResultSet();

            // Regards that the command is DDL.
            if (isDDLCommand(statement.getUpdateCount(),
                resultSet.getMetaData().getColumnCount())) {
              interpreterResult.add(Type.TEXT,
                  "Query executed successfully.");
            } else {
              MutableBoolean isComplete = new MutableBoolean(true);
              String results = getResults(resultSet,
                  !containsIgnoreCase(sqlToExecute, EXPLAIN_PREDICATE.getValue()), isComplete);
              interpreterResult.add(results);
              if (!isComplete.booleanValue()) {
                interpreterResult.add(ResultMessages.getExceedsLimitRowsMessage(getMaxResult(),
                    String.format("%s.%s", COMMON_KEY, MAX_LINE_KEY)));
              }
            }
          } else {
            // Response contains either an update count or there are no results.
            int updateCount = statement.getUpdateCount();
            interpreterResult.add(Type.TEXT,
                "Query executed successfully. Affected rows : " +
                    updateCount);
          }
        } finally {
          if (resultSet != null) {
            try {
              resultSet.close();
            } catch (SQLException e) { /*ignored*/ }
          }
          try {
            statement.close();
          } catch (SQLException e) { /*ignored*/ }
        }
      }
    } catch (Exception e) {
      logger.error("Cannot run " + sql, e);
      String errorMsg = ExceptionUtils.getStackTrace(e);
      interpreterResult.add(errorMsg);
      return new InterpreterResult(Code.ERROR, interpreterResult.message());
    } finally {
      //In case user ran an insert/update/upsert statement
      try {
        if (!connection.getAutoCommit()) {
          connection.commit();
        }
        connection.close();
      } catch (SQLException e) { /*ignored*/ }
      getJDBCConfiguration(user).removeStatement(paragraphId);
    }
    return interpreterResult;
  }

  /**
   * For %table response replace Tab and Newline characters from the content.
   */
  private String replaceReservedChars(String str) {
    if (str == null) {
      return EMPTY_COLUMN_VALUE.getValue();
    }
    return str.replace(TAB.getValue(), WHITESPACE.getValue())
        .replace(NEWLINE.getValue(), WHITESPACE.getValue());
  }

  @Override
  public InterpreterResult interpret(String originalCmd, InterpreterContext contextInterpreter) {
    String cmd = Boolean.parseBoolean(getProperty("zeppelin.jdbc.interpolation")) ?
        interpolate(originalCmd, contextInterpreter.getResourcePool()) : originalCmd;
    logger.debug("Run SQL command '{}'", cmd);
    String propertyKey = getPropertyKey(contextInterpreter);
    cmd = cmd.trim();
    logger.debug("PropertyKey: {}, SQL command: '{}'", propertyKey, cmd);
    return executeSql(propertyKey, cmd, contextInterpreter);
  }

  @Override
  public void cancel(InterpreterContext context) {
    logger.info("Cancel current query statement.");
    String paragraphId = context.getParagraphId();
    JDBCUserConfigurations jdbcUserConfigurations =
        getJDBCConfiguration(context.getAuthenticationInfo().getUser());
    try {
      jdbcUserConfigurations.cancelStatement(paragraphId);
    } catch (SQLException e) {
      logger.error("Error while cancelling...", e);
    }
  }

  public String getPropertyKey(InterpreterContext interpreterContext) {
    Map<String, String> localProperties = interpreterContext.getLocalProperties();
    // It is recommended to use this kind of format: %jdbc(db=mysql)
    if (localProperties.containsKey("db")) {
      return localProperties.get("db");
    }
    // %jdbc(mysql) is only for backward compatibility
    for (Map.Entry<String, String> entry : localProperties.entrySet()) {
      if (entry.getKey().equals(entry.getValue())) {
        return entry.getKey();
      }
    }
    return DEFAULT_KEY.getValue();
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    String schedulerName = JDBCInterpreter.class.getName() + this.hashCode();
    return isConcurrentExecution() ?
        SchedulerFactory.singleton().createOrGetParallelScheduler(schedulerName,
            getMaxConcurrentConnection())
        : SchedulerFactory.singleton().createOrGetFIFOScheduler(schedulerName);
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
      InterpreterContext interpreterContext) throws InterpreterException {
    List<InterpreterCompletion> candidates = new ArrayList<>();
    String propertyKey = getPropertyKey(interpreterContext);
    String sqlCompleterKey =
        String.format("%s.%s", interpreterContext.getAuthenticationInfo().getUser(), propertyKey);
    SqlCompleter sqlCompleter = sqlCompletersMap.get(sqlCompleterKey);

    Connection connection = null;
    try {
      if (interpreterContext != null) {
        connection = getConnection(propertyKey, interpreterContext);
      }
    } catch (ClassNotFoundException | SQLException | IOException e) {
      logger.warn("SQLCompleter will created without use connection", e);
    }

    sqlCompleter = createOrUpdateSqlCompleter(sqlCompleter, connection, propertyKey, buf, cursor);
    sqlCompletersMap.put(sqlCompleterKey, sqlCompleter);
    sqlCompleter.fillCandidates(buf, cursor, candidates);

    return candidates;
  }

  private int getMaxResult() {
    return maxLineResults;
  }

  boolean isConcurrentExecution() {
    return Boolean.valueOf(getProperty(CONCURRENT_EXECUTION_KEY.getValue()));
  }

  int getMaxConcurrentConnection() {
    try {
      return Integer.valueOf(getProperty(CONCURRENT_EXECUTION_COUNT.getValue()));
    } catch (Exception e) {
      return 10;
    }
  }
}
