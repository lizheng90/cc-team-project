package com.penguin.undertow;

import io.undertow.server.HttpServerExchange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;

/**
 * Question 3 MySQL Handler.
 */

public class Q3MySQLHandler extends BaseHttpHandler {

  private static final char[] FLAG_MAP = new char[] { '0', '+', '-', '*' };

  /**
   * Keys for http params
   */
  private static final String KEY_UID = "userid";

  @Override
  public String getResponse(HttpServerExchange exchange) {
    // Get parameters
    Deque<String> uid = exchange.getQueryParameters().get(KEY_UID);

    if (uid == null || uid.isEmpty()) {
      return getDefaultResponse();
    }

    String userId = uid.peekFirst();

    if (userId == null) {
      return getDefaultResponse();
    }

    StringBuilder response = new StringBuilder();
    String sql = "SELECT * FROM twitter3 WHERE sourceid='" + userId
        + "'ORDER BY FIELD(flag,3,1,2), count DESC, retweetid ASC;";

    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try {
      conn = ConnectionUtils.getInstance().getMySQLConnection();
      pstmt = conn.prepareStatement(sql);
      rs = pstmt.executeQuery();

      while (rs.next()) {
        char relationship = FLAG_MAP[rs.getInt("flag")];
        response = response.append(relationship).append(",")
            .append(rs.getString("count")).append(",")
            .append(rs.getString("retweetid")).append("\n");
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
}
