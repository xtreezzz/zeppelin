package org.apache.zeppelin;

import org.flywaydb.core.Flyway;

class FlywayMigrationInitializer {

  static void startMigrations(final String url, final String username,
      final String password) {
    Flyway
        .configure()
        .dataSource(url, username, password)
        .load()
        .migrate();
  }
}
