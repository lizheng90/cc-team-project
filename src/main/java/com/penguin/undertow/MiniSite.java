package com.penguin.undertow;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
// hbase api import
// dynamodb api import

/**
 * @author mitayun
 */
public class MiniSite {
  /**
   * Database type
   */
  static final int MYSQL = 0;
  static final int HBASE = 1;
  static final int DB_TYPE = MYSQL;

  private static final String PATH_HC = "/healthcheck";
  private static final String PATH_Q1 = "/q1";
  private static final String PATH_Q2 = "/q2";
  private static final String PATH_Q3 = "/q3";
  private static final String PATH_Q4 = "/q4";

  static final int SERVER_PORT = 80;
  static final String SERVER_IP = "0.0.0.0";


  @SuppressWarnings("all")
  public static void main(String[] args) throws Exception {

    Undertow.Builder builder = Undertow.builder().addHttpListener(SERVER_PORT,
        SERVER_IP);

    HttpHandler healthCheckHandler = new HealthCheckHandler();
    HttpHandler q1Handler = new Q1HeartBeatHandler();
    HttpHandler q2Handler = null;
    if (DB_TYPE == MYSQL) {
      q2Handler = new Q2MySQLHandler();
    } else {
      q2Handler = new Q2HBaseHandler();
    }
    HttpHandler q3Handler = new Q3MySQLHandler();
    HttpHandler q4Handler = new Q4MySQLHandler();

    builder.setHandler(Handlers.path()
        .addPrefixPath(PATH_HC, healthCheckHandler)
        .addPrefixPath(PATH_Q1, q1Handler)
        .addPrefixPath(PATH_Q2, q2Handler)
        .addPrefixPath(PATH_Q3, q3Handler)
        .addPrefixPath(PATH_Q4, q4Handler));

    Undertow server = builder.build();
    server.start();
    System.out.println("Server started");
  }
}