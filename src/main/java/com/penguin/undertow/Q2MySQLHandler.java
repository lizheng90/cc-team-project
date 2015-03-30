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
    long endTime;

    // CallableStatement cs = null;
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try {
      conn = HikariConnectionPool.getInstance().getConnection();

      System.out.println(sql);

      /*pstmt = conn
          .prepareStatement("select * from CC_Final.twitter2 where userid = ? and ts = ?");
      pstmt.setString(1, userId);
      pstmt.setString(2, timeStamp);*/
      pstmt = conn.prepareStatement(sql);

      endTime = System.currentTimeMillis();
      System.out.println("1Total execution time: " + (endTime - startTime)
          + "ms");

      rs = pstmt.executeQuery();
      // pstmt = ConnectionUtils.getInstance().getMySQLConnection()
      // .prepareStatement(sql);

      /*cs = ConnectionUtils.getInstance().getMySQLConnection()
          .prepareCall("{call query_twitter(?,?,?)}");*/
      /*cs = ConnectionPool.getInstance().getConnection()
          .prepareCall("{call query_twitter(?,?,?)}");
      cs.setLong(1, Long.parseLong(userId));
      cs.setLong(2, Long.parseLong(timeStamp));
      cs.registerOutParameter(3, Types.VARCHAR);
      cs.executeUpdate();
      */

      endTime = System.currentTimeMillis();
      System.out.println("2Total execution time: " + (endTime - startTime)
          + "ms");
      // rs = pstmt.executeQuery();
      // rs = cs.getResultSet();
      endTime = System.currentTimeMillis();
      System.out.println("3Total execution time: " + (endTime - startTime)
          + "ms");
      if (rs != null) {
        while (rs.next()) {
          // response = response + cs.getString(3);
          response = response + rs.getString(3);
        }
      }

      endTime = System.currentTimeMillis();
      System.out.println("4Total execution time: " + (endTime - startTime)
          + "ms");

      if (rs != null) {
        rs.close();
      }
      if (pstmt != null) {
        pstmt.close();
      }
      if (conn != null) {
        conn.close();
      }

      endTime = System.currentTimeMillis();
      System.out.println("5Total execution time: " + (endTime - startTime)
          + "ms");
    } catch (SQLException e) {
      e.printStackTrace();
    }

    String result = response.replaceAll("\t", "");
    return getDefaultResponse() + result;
  }
}
