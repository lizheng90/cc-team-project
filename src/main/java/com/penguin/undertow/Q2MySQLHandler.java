package com.penguin.undertow;

import io.undertow.server.HttpServerExchange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;

/**
 * Handler for Question 2 : JDBC Query
 */

public class Q2MySQLHandler extends BaseHttpHandler {

  private static final boolean DEBUG = false;

  /**
   * Keys for http params
   */
  private static final String KEY_UID = "userid";
  private static final String KEY_TIME = "tweet_time";

  @Override
  public String getResponse(HttpServerExchange exchange) {
    // Get parameters
    Deque<String> uid = exchange.getQueryParameters().get(KEY_UID);
    Deque<String> time = exchange.getQueryParameters().get(KEY_TIME);

    if (uid == null || time == null || uid.isEmpty() || time.isEmpty()) {
      return getDefaultResponse();
    }

    String userId = uid.peekFirst();
    String timeStamp = time.peekFirst().replaceAll("   | |-|:|\t", "");

    if (userId == null || timeStamp == null) {
      return getDefaultResponse();
    }

    String response = "";
    String sql = "SELECT * FROM CC_Final.twitter2 WHERE userid='" + userId
        + "' AND ts='" + timeStamp + "';";
    long startTime = System.currentTimeMillis();

    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try {
      conn = ConnectionUtils.getInstance().getMySQLConnection();

      if (DEBUG) {
        System.out.println(sql);
      }

      pstmt = conn.prepareStatement(sql);

      if (DEBUG) {
        logTime(startTime, 1);
      }

      rs = pstmt.executeQuery();

      if (DEBUG) {
        logTime(startTime, 2);
      }

      if (rs != null) {
        while (rs.next()) {
          response = response + rs.getString(3);
        }
      }

      if (DEBUG) {
        logTime(startTime, 3);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (pstmt != null) {
          pstmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e) {
        System.out.println("exception when closing");
      }
    }

    if (DEBUG) {
      logTime(startTime, 4);
    }
    String result = response.replaceAll("\t", "");
    return getDefaultResponse() + result;
  }

  private void logTime(long startTime, int key) {
    long endTime = System.currentTimeMillis();
    System.out
        .println("1Total execution time: " + (endTime - startTime) + "ms");
  }
}
