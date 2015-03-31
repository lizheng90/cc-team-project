package com.penguin.undertow;

import io.undertow.server.HttpServerExchange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;

/**
 * Question 4 MySQL Handler.
 */

public class Q4MySQLHandler extends BaseHttpHandler {

  /**
   * Keys for http params
   */
  private static final String KEY_HASHTAG = "hashtag";
  private static final String KEY_START = "start";
  private static final String KEY_END = "end";

  @Override
  public String getResponse(HttpServerExchange exchange) {
    // Get parameters
    Deque<String> hashtag = exchange.getQueryParameters().get(KEY_HASHTAG);
    Deque<String> start = exchange.getQueryParameters().get(KEY_START);
    Deque<String> end = exchange.getQueryParameters().get(KEY_END);

    if (hashtag == null || hashtag.isEmpty() || start == null
        || start.isEmpty() || end == null || end.isEmpty()) {
      return getDefaultResponse();
    }

    String tag = hashtag.peekFirst();
    String starttime = start.peekFirst();
    String endtime = end.peekFirst();

    if (tag == null || starttime == null || endtime == null) {
      return getDefaultResponse();
    }
    starttime = starttime.replaceAll("-", "") + "000000";
    endtime = endtime.replaceAll("-", "") + "235959";

    StringBuilder response = new StringBuilder();
    String sql = "SELECT DISTINCT * FROM twitter4 WHERE tag='" + tag
        + "' AND time BETWEEN '" + starttime + "' AND '" + endtime
        + "' ORDER BY tweetid ASC;";

    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try {
      conn = ConnectionUtils.getInstance().getMySQLConnection();
      pstmt = conn.prepareStatement(sql);
      rs = pstmt.executeQuery();

      while (rs.next()) {
        String thisTime = getTime(rs.getString("time"));
        response = response.append(rs.getString("tweetid")).append(",")
            .append(rs.getString("userid")).append(",").append(thisTime);
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

    String result = response.toString();
    return getDefaultResponse() + result;
  }

  public String getTime(String str) {
    String result = str.substring(0, 4) + "-" + str.substring(4, 6) + "-"
        + str.substring(6, 8) + "+" + str.substring(8, 10) + ":"
        + str.substring(10, 12) + ":" + str.substring(12, 14) + "\n";
    return result;
  }
}
