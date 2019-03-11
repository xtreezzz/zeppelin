package org.apache.zeppelin;

import javax.annotation.PostConstruct;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
class FlywayMigrationInitializer {

  @Autowired
  private Environment environment;

  @PostConstruct
  private void startMigrations() {
    String url = environment.getProperty("spring.datasource.url");
    String username = environment.getProperty("spring.datasource.username");
    String password = environment.getProperty("spring.datasource.password");
    Flyway
        .configure()
        .dataSource(url, username, password)
        .load()
        .migrate();
  }
}
