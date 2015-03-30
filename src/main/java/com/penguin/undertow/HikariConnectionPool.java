package com.penguin.undertow;

import java.sql.Connection;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HikariConnectionPool {

  private static HikariConnectionPool instance = null;
  private HikariDataSource dataSource = null;

  private HikariConnectionPool() {
    HikariConfig config = new HikariConfig();
    config.setMaximumPoolSize(150);
    config
        .setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
    config.addDataSourceProperty("port", 3306);
    config.addDataSourceProperty("serverName", "localhost");
    config.addDataSourceProperty("user", "root");
    config.addDataSourceProperty("password", "");
    config.addDataSourceProperty("url", "jdbc:mysql://localhost:3306/CC_Final");
    dataSource = new HikariDataSource(config);
  }

  public static HikariConnectionPool getInstance() {
    if (instance == null) {
      instance = new HikariConnectionPool();
    }
    return instance;
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }
}
