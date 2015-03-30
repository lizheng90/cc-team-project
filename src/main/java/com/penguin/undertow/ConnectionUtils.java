package com.penguin.undertow;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author mitayun
 */
public class ConnectionUtils {

  private static ConnectionUtils instance = null;

  /**
   * MySQL
   */

  private static final String ADDRESS_MYSQL = "jdbc:mysql://localhost:3306/CC_Final";
  private static final String USERNAME = "root";
  private static final String PW = "";
  private static Connection mySqlConnection = null;

  /**
   * HBase
   */
  private static final String ADDRESS_HBASE = "hdfs://172.31.58.237:9000";
  private static final String ZOOKEEPER = "ip-172-31-58-237.ec2.internal";
  private static final String TABLE_NAME = "friendslist";
  private static HTable hTable = null;

  @SuppressWarnings("all")
  protected ConnectionUtils() {
    // MySQL
    if (MiniSite.DB_TYPE == MiniSite.MYSQL) {
      try {
        mySqlConnection = DriverManager.getConnection(ADDRESS_MYSQL, USERNAME,
            PW);
        System.out.println("Connected to MySQL");
      } catch (SQLException e) {
        e.printStackTrace();
      }
    } else if (MiniSite.DB_TYPE == MiniSite.HBASE) {

      // HBase
      Configuration config = HBaseConfiguration.create();
      config.set("hbase.master", ADDRESS_HBASE);
      config.set("hbase.zookeeper.quorum", ZOOKEEPER);
      try {
        hTable = new HTable(config, Bytes.toBytes(TABLE_NAME));
        System.out.println("Connected to HTable");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static ConnectionUtils getInstance() {
    if (instance == null) {
      instance = new ConnectionUtils();
    }
    return instance;
  }

  public Connection getMySQLConnection() {
    return mySqlConnection;
  }

  public HTable getHTable() {
    return hTable;
  }
}
