package org.apache.zeppelin.storage;

import com.google.common.base.Preconditions;
import org.apache.zeppelin.Repository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class ModuleRepositoryDAO {

  private static final String GET_ALL_REPOSITORIES = "SELECT * FROM repository";

  private static final String GET_REPOSITORY = "SELECT * FROM repository WHERE repository_id = "
          + ":repository_id";

  private static final String PERSIST = "INSERT INTO repository(repository_id, snapshot, "
          + "url, username, password, proxy_protocol, proxy_host, proxy_port, proxy_login, "
          + "proxy_password) VALUES (:repository_id, :snapshot, :url, :username, :password, "
          + ":proxy_protocol, :proxy_host, :proxy_port, :proxy_login, :proxy_password)";


  private static final String DELETE_REPOSITORY = "DELETE FROM Repository WHERE "
          + "repository_id = :repository_id";


  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public ModuleRepositoryDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }


  @Nonnull
  private MapSqlParameterSource convertRepositoryToParameters(
          @Nonnull final Repository source) {
    Preconditions.checkNotNull(source);
    final MapSqlParameterSource parameters = new MapSqlParameterSource();
    try {
      parameters
              .addValue("repository_id", source.getId())
              .addValue("snapshot", source.isSnapshot())
              .addValue("url", source.getUrl())
              .addValue("username", source.getUsername())
              .addValue("password", source.getPassword())
              .addValue("proxy_protocol",
                      source.getProxyProtocol() != null ? source.getProxyProtocol().name() : null)
              .addValue("proxy_host", source.getProxyHost())
              .addValue("proxy_port", source.getProxyPort())
              .addValue("proxy_login", source.getProxyLogin())
              .addValue("proxy_password", source.getProxyPassword());
    } catch (final Exception e) {
      throw new RuntimeException("Fail to convert repository", e);
    }
    Preconditions.checkNotNull(parameters);
    return parameters;
  }

  @Nonnull
  private static Repository mapRow(final ResultSet resultSet, final int i) throws SQLException {
    return new Repository(resultSet.getBoolean("snapshot"),
            resultSet.getString("repository_id"),
            resultSet.getString("url"),
            resultSet.getString("username"),
            resultSet.getString("password"),
            Repository.ProxyProtocol.valueOf(resultSet.getString("proxy_protocol")),
            resultSet.getString("proxy_host"),
            resultSet.getInt("proxy_port"),
            resultSet.getString("proxy_login"),
            resultSet.getString("proxy_password"));
  }


  public List<Repository> getAll() {
    final SqlParameterSource parameters = new MapSqlParameterSource();

    return namedParameterJdbcTemplate.query(
            GET_ALL_REPOSITORIES,
            parameters,
            ModuleRepositoryDAO::mapRow);
  }

  @Nullable
  public Repository getById(@Nonnull final String id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("repository_id", id);

    return namedParameterJdbcTemplate.query(
            GET_REPOSITORY,
            parameters,
            ModuleRepositoryDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public void persist(@Nonnull final Repository repository) {
    namedParameterJdbcTemplate.update(PERSIST, convertRepositoryToParameters(repository));
  }

  public void delete(@Nonnull final String id) {
    namedParameterJdbcTemplate.update(DELETE_REPOSITORY, new MapSqlParameterSource("repository_id", id));
  }
}
