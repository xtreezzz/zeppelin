package ru.tinkoff.zeppelin.jdbc;

/*
 * This source file is based on code taken from SQLLine 1.0.2 See SQLLine notice in LICENSE
 */
import java.util.Collections;
import java.util.HashSet;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.zeppelin.completer.CachedCompleter;
import org.apache.zeppelin.completer.CompletionType;
import org.apache.zeppelin.completer.StringsCompleter;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;

/**
 * SQL auto complete functionality for the JdbcInterpreter.
 */
public class SqlCompleter {

  private static Logger logger = LoggerFactory.getLogger(SqlCompleter.class);

  /**
   * Completer for sql keywords.
   */
  private CachedCompleter<StringsCompleter> keywordCompleter;

  /**
   * Schema completer.
   */
  private CachedCompleter<StringsCompleter> schemasCompleter;

  /**
   * Contain different completer with table list for every schema name.
   */
  private Map<String, CachedCompleter<StringsCompleter>> tablesCompleters = new HashMap<>();

  /**
   * Contain different completer with table list for every schema name.
   */
  private Map<String, CachedCompleter<StringsCompleter>> viewsCompleters = new HashMap<>();

  /**
   * Contains different completer with column list for every table name.
   * Table names store as schema_name.table_name
   */
  private Map<String, CachedCompleter<StringsCompleter>> columnsCompleters = new HashMap<>();

  /**
   * Contains description of each database entity. Key - full name of entity.
   */
  private static Map<String, String> databaseEntityDescription = new HashMap<>();

  private static boolean loadMeta = true;

  private int ttlInSeconds;

  private String defaultSchema;

  SqlCompleter(int ttlInSeconds) {
    this.ttlInSeconds = ttlInSeconds;
  }

  private static String extractEntityDescription(ResultSet entityInfo) {
    String description = StringUtils.EMPTY;
    try {
      description  = entityInfo.getString("REMARKS");
      if (description == null) {
        description  = StringUtils.EMPTY;
      }
    } catch (Exception e) {
      logger.error("Failed to get info", e);
    }
    return description;
  }

  /**
   * Return schema for tables that can be written without schema in query.
   * Typically it is enough to use getSchema() on connection,
   * but for Oracle - getUserName() from DatabaseMetaData.
   */
  private String getDefaultSchema(Connection conn, DatabaseMetaData meta) {
    String defaultSchema = null;
    try {
      if ((defaultSchema = conn.getSchema()) == null) {
        defaultSchema = conn.getCatalog();
      }
    } catch (SQLException | AbstractMethodError e) {
      logger.debug("Default schema is not defined", e);
      try {
        defaultSchema = meta.getUserName();
      } catch (Exception ee) {
        logger.debug("User name is not defined", ee);
      }
    }
    return defaultSchema;
  }

  /**
   * Return list of schema names within the database.
   *
   * @param meta          metadata from connection to database
   * @param schemaFilters a schema name patterns; must match the schema name
   *                      as it is stored in the database; "" retrieves those without a schema;
   *                      <code>null</code> means that the schema name should not be used to narrow
   *                      the search; supports '%'; for example "prod_v_%"
   * @return set of all schema names in the database
   */
  private static Set<String> getSchemaNames(DatabaseMetaData meta, List<String> schemaFilters) {
    Set<String> res = new HashSet<>();
    try (ResultSet schemas = meta.getSchemas()) {
      while (schemas.next()) {
        String schemaName = schemas.getString("TABLE_SCHEM");
        if (schemaName == null) {
          schemaName = "";
        }
        String schemaDescription = extractEntityDescription(schemas);
        for (String schemaFilter : schemaFilters) {
          if (schemaFilter.equals("") || schemaName.matches(schemaFilter.replace("%",
              ".*?"))) {
            res.add(schemaName);
            databaseEntityDescription.put(schemaName, schemaDescription);
          }
        }
      }
    } catch (SQLException t) {
      logger.error("Failed to retrieve the schema names", t);
    }
    return res;
  }

  /**
   * Return list of catalog names within the database.
   *
   * @param meta          metadata from connection to database
   * @param schemaFilters a catalog name patterns; must match the catalog name
   *                      as it is stored in the database; "" retrieves those without a catalog;
   *                      <code>null</code> means that the schema name should not be used to narrow
   *                      the search; supports '%'; for example "prod_v_%"
   * @return set of all catalog names in the database
   */
  private static Set<String> getCatalogNames(DatabaseMetaData meta, List<String> schemaFilters) {
    Set<String> res = new HashSet<>();
    try {
      try (ResultSet schemas = meta.getCatalogs()) {
        while (schemas.next()) {
          String schemaName = schemas.getString("TABLE_CAT");
          String schemaDescription = extractEntityDescription(schemas);
          for (String schemaFilter : schemaFilters) {
            if (schemaFilter.equals("") || schemaName.matches(schemaFilter.replace("%",
                ".*?"))) {
              res.add(schemaName);
              databaseEntityDescription.put(schemaName, schemaDescription);
            }
          }
        }
      }
    } catch (SQLException t) {
      logger.error("Failed to retrieve the schema names", t);
    }
    return res;
  }

  private static void fillViewsNames(String schema, DatabaseMetaData meta, Set<String> views) {
    try (ResultSet tbls = meta.getTables(schema, schema, "%",
        new String[]{"VIEW"})) {
      if (!tbls.isBeforeFirst()) {
        logger.debug("There is no views for schema {}", schema);
      } else {
        while (tbls.next()) {
          logger.info("\n[DEBUG]\n {}", tbls);
          String table = tbls.getString("TABLE_NAME");
          String tableDescription = extractEntityDescription(tbls);
          views.add(schema + "." + table);
          databaseEntityDescription.put(schema + "." + table, tableDescription);
        }
      }
    } catch (Exception t) {
      logger.error("Failed to retrieve the table name", t);
    }
  }

  private static void fillTableNames(String schema, DatabaseMetaData meta, Set<String> tables) {
    try (ResultSet tbls = meta.getTables(schema, schema, "%",
        new String[]{"TABLE", "ALIAS", "SYNONYM", "GLOBAL TEMPORARY",
            "LOCAL TEMPORARY"})) {
      if (!tbls.isBeforeFirst()) {
        logger.debug("There is no tables for schema {}", schema);
      } else {
        while (tbls.next()) {
          String table = tbls.getString("TABLE_NAME");
          String tableDescription = extractEntityDescription(tbls);
          tables.add(schema + "." + table);
          databaseEntityDescription.put(schema + "." + table, tableDescription);
        }
      }
    } catch (Exception t) {
      logger.error("Failed to retrieve the table name", t);
    }
  }

  /**
   * Fill two map with list of tables and list of columns.
   *
   * @param schema  name of a scheme
   * @param table   name of a table
   * @param meta    meta metadata from connection to database
   * @param columns function fills this set, for every table name adds set
   *                of columns within the table; table name is in format schema_name.table_name
   */
  private static void fillColumnNames(String schema, String table, DatabaseMetaData meta,
                                      Set<String> columns) {
    try (ResultSet cols = meta.getColumns(schema, schema, table, "%")) {
      while (cols.next()) {
        String column = cols.getString("COLUMN_NAME");
        String columnDescription = extractEntityDescription(cols);
        columns.add(schema + "." + table + "." + column);
        databaseEntityDescription.put(schema + "." + table + "." + column, columnDescription);
      }
    } catch (Exception t) {
      logger.error("Failed to retrieve the column name", t);
    }
  }

  private static boolean getSqlKeywordsCompletions(Set<String> completions, String statement) {
    boolean loadMeta = false;
    if (statement.trim().endsWith(".") || statement.trim().endsWith("=")
        || statement.trim().endsWith("where") || statement.trim().endsWith("from")) {
      loadMeta = true;
    }
    StringBuilder myBuf = new StringBuilder();
    SelectDeParser selectDeparser = new SelectDeParser();
    selectDeparser.setBuffer(myBuf);
    ExpressionDeParser expressionDeParser =
        new ExpressionDeParser(selectDeparser, new StringBuilder());
    StatementDeParser statementDeParser =
        new StatementDeParser(expressionDeParser, selectDeparser, new StringBuilder());

    try {
      List<String> statements = Arrays.asList(statement.split(";"));
      statement = statements.get(statements.size() - 1);

      Statement parseExpression = CCJSqlParserUtil.parse(statement);
      parseExpression.accept(statementDeParser);

      // Remove keywords used in last inner select statement from keyword list
      List<String> innerSelects = Arrays.asList(statement.split("\\("));
      List<String> words = Arrays.asList(innerSelects.get(innerSelects.size() - 1).split(" "));
      List<String> keywords = Arrays.asList("from", "where", "select", "join", "left", "right",
          "inner", "outer", "on", "and", "or");
      keywords.removeAll(words);
      completions.addAll(keywords);
    } catch (JSQLParserException e) {
      if (e.getCause().toString().contains("Was expecting one of:")) {
        List<String> expected = Arrays.asList(e.getCause().toString()
            .substring(e.getCause().toString().indexOf("Was expecting one of:")).split("\n"));
        expected = expected.subList(1, expected.size());
        for (String expectedValue : expected) {
          expectedValue =
              expectedValue.trim().replace("\"", "").toLowerCase();

          if (expectedValue.equalsIgnoreCase("<S_IDENTIFIER>")) {
            loadMeta = true;
          }
          if (!expectedValue.startsWith("<") && !expectedValue.startsWith("{")) {
            completions.add(expectedValue);
          }
        }
        if (completions.contains("*")) {
          completions.add("from");
        }
      }
    }
    return loadMeta;
  }

  private SqlStatement getStatementParameters(String buffer, int cursor) {
    Collection<String> schemas = Collections.emptyList();
    if (schemasCompleter != null) {
      schemas = schemasCompleter.getCompleter().getStrings();
    }
    Collection<String> keywords = Collections.emptyList();
    if (keywordCompleter != null) {
      keywords = keywordCompleter.getCompleter().getStrings();
    }

    Collection<String> tablesInDefaultSchema = new TreeSet<>();
    if (tablesCompleters.containsKey(defaultSchema)) {
      tablesInDefaultSchema = tablesCompleters.get(defaultSchema).getCompleter().getStrings();
    }
    if (viewsCompleters.containsKey(defaultSchema)) {
      tablesInDefaultSchema.addAll(viewsCompleters.get(defaultSchema).getCompleter().getStrings());
    }


    return new SqlStatement(buffer, cursor, defaultSchema, schemas,
        tablesInDefaultSchema, keywords);
  }

  /**
   * Initializes all local completers from database connection.
   *
   * @param connection          database connection
   * @param schemaFiltersString a comma separated schema name patterns, supports '%'  symbol;
   *                            for example "prod_v_%,prod_t_%"
   */

  void createOrUpdateFromConnection(Connection connection, String schemaFiltersString,
                                    String buffer, int cursor) {
    // get statement completion
    Set<String> keywords = new TreeSet<>();
    loadMeta = getSqlKeywordsCompletions(keywords, buffer);
    initKeywords(keywords);
    logger.info("Keyword completer initialized with {} keywords", keywords.size());

    try (Connection c = connection) {
      if (schemaFiltersString == null) {
        schemaFiltersString = StringUtils.EMPTY;
      }
      List<String> schemaFilters = Arrays.asList(schemaFiltersString.split(","));


      if (c != null) {
        DatabaseMetaData databaseMetaData = c.getMetaData();

        //TODO(mebelousov): put defaultSchema in cache
        if (defaultSchema == null) {
          defaultSchema = getDefaultSchema(connection, databaseMetaData);
        }

        if (schemasCompleter == null || schemasCompleter.getCompleter() == null
            || schemasCompleter.isExpired()) {
          Set<String> schemas = getSchemaNames(databaseMetaData, schemaFilters);
          Set<String> catalogs = getCatalogNames(databaseMetaData, schemaFilters);

          if (schemas.size() == 0) {
            schemas.addAll(catalogs);
          }

          if (!loadMeta) {
            schemas.clear();
          }

          initSchemas(schemas);
          logger.info("Schema completer initialized with " + schemas.size() + " schemas");
        }

        CachedCompleter<StringsCompleter> tablesCompleterInDefaultSchema = tablesCompleters
            .get(defaultSchema);

        if (tablesCompleterInDefaultSchema == null || tablesCompleterInDefaultSchema.isExpired()) {
          Set<String> tables = new HashSet<>();
          if (loadMeta) {
            fillTableNames(defaultSchema, databaseMetaData, tables);
          }
          initTables(defaultSchema, tables);
        }

        CachedCompleter<StringsCompleter> viewsCompleterInDefaultSchema = viewsCompleters
            .get(defaultSchema);

        if (viewsCompleterInDefaultSchema == null || viewsCompleterInDefaultSchema.isExpired()) {
          Set<String> views = new HashSet<>();
          if (loadMeta) {
            fillViewsNames(defaultSchema, databaseMetaData, views);
          }
          initViews(defaultSchema, views);
        }

        SqlStatement sqlStatement = getStatementParameters(buffer, cursor);

        if (sqlStatement.needLoadTables()) {
          String schema = sqlStatement.getSchema();
          CachedCompleter tablesCompleter = tablesCompleters.get(schema);
          if (tablesCompleter == null || tablesCompleter.isExpired()) {
            Set<String> tables = new HashSet<>();
            if (loadMeta) {
              fillTableNames(schema, databaseMetaData, tables);
            }
            initTables(schema, tables);
            logger.info("Tables completer for schema " + schema + " initialized with "
                + tables.size() + " tables");
          }
          CachedCompleter viewsCompleter = viewsCompleters.get(schema);
          if (viewsCompleter == null || viewsCompleter.isExpired()) {
            Set<String> views = new HashSet<>();
            if (loadMeta) {
              fillViewsNames(schema, databaseMetaData, views);
            }
            initViews(schema, views);
            logger.info("Views completer for schema " + schema + " initialized with "
                + views.size() + " views");
          }
        }

        for (String schemaTable : sqlStatement.getActiveSchemaTables()) {
          CachedCompleter columnsCompleter = columnsCompleters.get(schemaTable);
          if (columnsCompleter == null || columnsCompleter.isExpired()) {
            int pointPos = schemaTable.indexOf('.');
            Set<String> columns = new HashSet<>();
            if (loadMeta) {
              fillColumnNames(schemaTable.substring(0, pointPos),
                  schemaTable.substring(pointPos + 1),
                  databaseMetaData, columns);
            }
            initColumns(schemaTable, columns);
            logger.info("Completer for schemaTable " + schemaTable + " initialized with "
                + columns.size() + " columns.");
          }
        }
      }
    } catch (SQLException e) {
      logger.error("Failed to update the metadata completions" + e.getMessage());
    }
  }

  void initKeywords(Set<String> keywords) {
    if (keywords != null && !keywords.isEmpty()) {
      keywordCompleter = new CachedCompleter(new StringsCompleter(keywords), 0);
    }
  }

  void initSchemas(Set<String> schemas) {
    if (schemas != null && !schemas.isEmpty()) {
      schemasCompleter = new CachedCompleter(
          new StringsCompleter(new TreeSet<>(schemas)), ttlInSeconds);
    }
  }

  void initTables(String schema, Set<String> tables) {
    if (tables != null && !tables.isEmpty()) {
      tablesCompleters.put(schema, new CachedCompleter(
          new StringsCompleter(new TreeSet<>(tables)), ttlInSeconds));
    }
  }

  void initViews(String schema, Set<String> views) {
    if (views != null && !views.isEmpty()) {
      viewsCompleters.put(schema, new CachedCompleter(
          new StringsCompleter(new TreeSet<>(views)), ttlInSeconds));
    }
  }

  void initColumns(String schemaTable, Set<String> columns) {
    if (columns != null && !columns.isEmpty()) {
      columnsCompleters.put(schemaTable,
          new CachedCompleter(new StringsCompleter(columns), ttlInSeconds));
    }
  }

  /**
   * Complete buffer in case it is a keyword.
   *
   * @return -1 in case of no candidates found, 0 otherwise
   */
  private int completeKeyword(String buffer, int cursor, List<CharSequence> candidates) {
    if (keywordCompleter == null || keywordCompleter.getCompleter() == null) {
      return -1;
    }
    return keywordCompleter.getCompleter().complete(buffer, cursor, candidates);
  }

  /**
   * Complete buffer in case it is a schema name.
   *
   * @return -1 in case of no candidates found, 0 otherwise
   */
  private int completeSchema(String buffer, int cursor, List<CharSequence> candidates) {
    if (schemasCompleter == null || schemasCompleter.getCompleter() == null) {
      return -1;
    }
    return schemasCompleter.getCompleter().complete(buffer, cursor, candidates);
  }

  /**
   * Complete buffer in case it is a table name.
   *
   * @return -1 in case of no candidates found, 0 otherwise
   */
  private int completeTable(String schema, String buffer, int cursor,
                            List<CharSequence> candidates) {
    // Wrong schema
    if (schema == null || tablesCompleters == null || !tablesCompleters.containsKey(schema)) {
      return -1;
    } else {
      return tablesCompleters.get(schema).getCompleter().complete(buffer, cursor, candidates);
    }
  }

  /**
   * Complete buffer in case it is a view name.
   *
   * @return -1 in case of no candidates found, 0 otherwise
   */
  private int completeView(String schema, String buffer, int cursor,
      List<CharSequence> candidates) {
    // Wrong schema
    if (schema == null || viewsCompleters == null || !viewsCompleters.containsKey(schema)) {
      return -1;
    } else {
      return viewsCompleters.get(schema).getCompleter().complete(buffer, cursor, candidates);
    }
  }

  /**
   * Complete buffer in case it is a column name.
   *
   * @return -1 in case of no candidates found, 0 otherwise
   */
  private int completeColumn(String schema, String table, String buffer, int cursor,
                             List<CharSequence> candidates) {
    // Wrong schema or wrong table
    if (schema == null || table == null || columnsCompleters == null ||
        !columnsCompleters.containsKey(schema + "." + table)) {
      return -1;
    } else {
      return columnsCompleters.get(schema + "." + table).getCompleter()
          .complete(buffer, cursor, candidates);
    }
  }

  /**
   * Fill candidates for statement.
   */
  void fillCandidates(String statement, int cursor, List<InterpreterCompletion> candidates) {
    SqlStatement sqlStatement = getStatementParameters(statement, cursor);

    logger.debug("Complete with buffer = " + statement + ", cursor = " + cursor);


    String schema = sqlStatement.getSchema();
    int cursorPosition = sqlStatement.getCursorPosition();

    if (schema == null) {   // process all
      final String buffer;
      if (cursorPosition > 0) {
        buffer = sqlStatement.getCursorString();
      } else {
        buffer = "";
      }

      List<CharSequence> keywordsCandidates = new ArrayList<>();
      completeKeyword(buffer, cursorPosition, keywordsCandidates);
      addCompletions(candidates, keywordsCandidates, CompletionType.keyword.name()
          + "-tinkoff");

      if (loadMeta) {
        int allColumnsRes = 0;
        List<CharSequence> columnCandidates = new ArrayList<>();
        for (String schemaTable : sqlStatement.getActiveSchemaTables()) {
          int pointPos = schemaTable.indexOf('.');
          int columnRes = completeColumn(schemaTable.substring(0, pointPos),
              schemaTable.substring(pointPos + 1), buffer, cursorPosition, columnCandidates);
          addCompletions(candidates, columnCandidates, CompletionType.column.name());
          allColumnsRes = allColumnsRes + columnRes;
        }

        List<CharSequence> tableInDefaultSchemaCandidates = new ArrayList<>();
        completeTable(defaultSchema, buffer, cursorPosition, tableInDefaultSchemaCandidates);
        addCompletions(candidates, tableInDefaultSchemaCandidates, CompletionType.table.name());

        List<CharSequence> viewInDefaultSchemaCandidates = new ArrayList<>();
        completeView(defaultSchema, buffer, cursorPosition, viewInDefaultSchemaCandidates);
        addCompletions(candidates, viewInDefaultSchemaCandidates, CompletionType.view.name());

        List<CharSequence> schemaCandidates = new ArrayList<>();
        completeSchema(buffer, cursorPosition, schemaCandidates);
        addCompletions(candidates, schemaCandidates, CompletionType.schema.name());
      }
    } else {
      String table = sqlStatement.getTable();
      String column = sqlStatement.getColumn();
      if (column == null) {
        List<CharSequence> tableCandidates = new ArrayList<>();
        int tableRes = completeTable(schema, table, cursorPosition, tableCandidates);
        addCompletions(candidates, tableCandidates, CompletionType.table.name());
        logger.debug("Complete for tables with " + tableRes + "candidates");

        List<CharSequence> viewCandidates = new ArrayList<>();
        int viewRes = completeView(schema, table, cursorPosition, viewCandidates);
        addCompletions(candidates, viewCandidates, CompletionType.view.name());
        logger.debug("Complete for views with " + viewRes + "candidates");
      } else { // process schema.table and alias case
        List<CharSequence> columnCandidates = new ArrayList<>();
        int columnRes = completeColumn(schema, table, column, cursorPosition, columnCandidates);
        addCompletions(candidates, columnCandidates, CompletionType.column.name());
        logger.debug("Complete for tables with " + columnRes + "candidates");
      }
    }
  }

  private void addCompletions(List<InterpreterCompletion> interpreterCompletions,
                              List<CharSequence> candidates, String meta) {
    for (CharSequence candidate : candidates) {
      String description = candidate.toString();
      // check candidate meta for description
      if (meta.equals(CompletionType.column.name())
          || meta.equals(CompletionType.schema.name())
          || meta.equals(CompletionType.table.name())) {
        description = databaseEntityDescription.get(candidate.toString());
        if (description == null || description.equals("")) {
          description = candidate.toString();
        }
      }

      List<String> path = Arrays.asList(candidate.toString().split("\\."));
      if (!path.isEmpty()) {
        candidate = path.get(path.size() - 1);
      }
      interpreterCompletions.add(
          new InterpreterCompletion(candidate.toString(), candidate.toString(), meta, description));
    }
  }
}
