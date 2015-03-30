package com.penguin.undertow;

import io.undertow.server.HttpServerExchange;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;

/**
 * Handler for Question 2 : JDBC Query
 */

public class Q2MySQLHandler extends BaseHttpHandler {

  /**
   * Keys for http params
   */
  private static final String KEY_UID = "userid";
  private static final String KEY_TIME = "tweet_time";

  private PreparedStatement pstmt = null;
  private ResultSet rs = null;

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
    String sql = "SELECT * FROM twitter2 WHERE userid='" + userId
        + "' AND ts='" + timeStamp + "';";
    long startTime = System.currentTimeMillis();
    long endTime;

    try {
      // Connection conn = getMySQLConnection();

      System.out.println(sql);

      endTime = System.currentTimeMillis();
      System.out.println("1Total execution time: " + (endTime - startTime)
          + "ms");

      pstmt = ConnectionUtils.getInstance().getMySQLConnection()
          .prepareStatement(sql);
      rs = pstmt.executeQuery();

      endTime = System.currentTimeMillis();
      System.out.println("2Total execution time: " + (endTime - startTime)
          + "ms");

      while (rs.next()) {
        response = response + rs.getString(3);
      }

      endTime = System.currentTimeMillis();
      System.out.println("3Total execution time: " + (endTime - startTime)
          + "ms");
      /*
      if (rs != null)
        rs.close();
      if (pstmt != null)
        pstmt.close();
      if (conn != null)
        conn.close();
      */
      endTime = System.currentTimeMillis();
      System.out.println("4Total execution time: " + (endTime - startTime)
          + "ms");
    } catch (SQLException e) {
      e.printStackTrace();
    }

    String result = response.replaceAll("\t", "");
    return getDefaultResponse() + result;
  }
}
