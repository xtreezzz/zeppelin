package org.apache.zeppelin.metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Generator metadata for JDBC
 */
public class JdbcMetadataGenerator implements MetadataGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(JdbcMetadataGenerator.class);

	private MetadataGeneratorSetting generatorSettings;
	private ZeppelinConfiguration zConf;

	@Override
	public void init(ZeppelinConfiguration zConf, MetadataGeneratorSetting generatorSettings)
			throws IOException {
		this.zConf = zConf;
		this.generatorSettings = generatorSettings;
	}

	@Override
	public MetadataNode generate() {
		Properties properties = generatorSettings.getProperties();
		String url = properties.getProperty("url");
		String user = properties.getProperty("user");
		String password = properties.getProperty("password");
		String driver = properties.getProperty("driver");
		String schemaFiltersString = properties.getProperty("schemaFilters");
		try {
			Class.forName(driver);
			try (Connection connection = DriverManager.getConnection(url, user, password)) {
				MetadataNode metadata = new MetadataNode();
				metadata.setType(MetadataType.root);
				List<String> schemasNames;
				List<String> tmpSchemasNames = new ArrayList<>();
				DatabaseMetaData databaseMetaData = connection.getMetaData();

				try (ResultSet schemas = databaseMetaData.getSchemas()) {
					while (schemas.next()) {
						String schemaName = schemas.getString("TABLE_SCHEM");
						tmpSchemasNames.add(schemaName);
					}
				}
				if (tmpSchemasNames.isEmpty()) {
					try (ResultSet catalogs = databaseMetaData.getCatalogs()) {
						while (catalogs.next()) {
							String catalogName = catalogs.getString("TABLE_CAT");
							tmpSchemasNames.add(catalogName);
						}
					}
				}
				if (StringUtils.isNotBlank(schemaFiltersString)) {
					schemasNames = new ArrayList<>();
					List<String> schemaFilters = Arrays.asList(schemaFiltersString.split(","));
					for (String schemaName : tmpSchemasNames) {
						for (String schemaFilter : schemaFilters) {
							if (schemaFilter.equals("") || schemaName.matches(schemaFilter.replace("%", ".*?"))) {
								schemasNames.add(schemaName);
							}
						}
					}
				} else {
					schemasNames = tmpSchemasNames;
				}
				if (schemasNames.isEmpty()) {
					return metadata;
				}
				MetadataNode shemas = getShemasTree(databaseMetaData, schemasNames);
				metadata.getChildren().add(shemas);
				return metadata;
			} catch (SQLException e) {
				LOG.error("Error create connection", e);
			}
		} catch (ClassNotFoundException e) {
			LOG.error("Not found driver %s", driver);
		}
		return null;
	}

	private MetadataNode getShemasTree(DatabaseMetaData databaseMetaData, List<String> schemasNames) {
		MetadataNode shemasRoot = new MetadataNode();
		shemasRoot.setType(MetadataType.root);
		shemasRoot.setName("shemas");
		for (String schemaName : schemasNames) {
			MetadataNode schemaMetaNode = new MetadataNode();
			schemaMetaNode.setType(MetadataType.schema);
			schemaMetaNode.setName(schemaName);
			try (ResultSet tables = databaseMetaData.getTables(schemaName, schemaName, "%",
					new String[]{"TABLE", "VIEW", "ALIAS", "SYNONYM", "GLOBAL TEMPORARY",
							"LOCAL TEMPORARY"})) {
				while (tables.next()) {
					String tableName = tables.getString("TABLE_NAME");
					MetadataNode tableMetaNode = new MetadataNode();
					tableMetaNode.setType(MetadataType.table);
					tableMetaNode.setName(tableName);
					try (ResultSet columns = databaseMetaData.getColumns(schemaName, schemaName,
							tableName, "%")) {
						while (columns.next()) {
							String columnName = columns.getString("COLUMN_NAME");
							MetadataNode columnMetaNode = new MetadataNode();
							columnMetaNode.setType(MetadataType.column);
							columnMetaNode.setName(columnName);
							tableMetaNode.getChildren().add(columnMetaNode);
						}
					} catch (Throwable t) {
						LOG.error("Failed to retrieve the column name", t);
					}
					schemaMetaNode.getChildren().add(tableMetaNode);
				}
			} catch (Throwable t) {
				LOG.error("Failed to retrieve the table name", t);
			}
			shemasRoot.getChildren().add(schemaMetaNode);
		}
		return shemasRoot;
	}
}
