package org.example;

import com.clickhouse.client.ClickHouseLoadBalancingPolicy;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.jdbc.ClickHouseDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.IntStream;

public class Main {

  public static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws SQLException {

    final Properties properties = new Properties();
    properties.setProperty(ClickHouseClientOption.COMPRESS.getKey(), "0");
    properties.setProperty(ClickHouseDefaults.USER.getKey(), "user");
    properties.setProperty(ClickHouseDefaults.PASSWORD.getKey(), "password");
    properties.setProperty(ClickHouseClientOption.LOAD_BALANCING_POLICY.getKey(), ClickHouseLoadBalancingPolicy.ROUND_ROBIN);
    properties.setProperty(ClickHouseHttpOption.CONNECTION_PROVIDER.getKey(), HttpConnectionProvider.APACHE_HTTP_CLIENT.name());
    properties.setProperty(ClickHouseHttpOption.MAX_OPEN_CONNECTIONS.getKey(), "3");

    final ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource("jdbc:clickhouse:https://example.org:8443", properties);

    IntStream.range(0, 10).parallel().forEach(x -> execQuery(clickHouseDataSource));
  }


  private static void execQuery(ClickHouseDataSource dataSource) {
    try (
            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement("select 1");
            ResultSet resultSet = statement.executeQuery()
    ) {
      while (resultSet.next()) {
        log.info("{}: retrieved 1 row", Thread.currentThread().getName());
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
