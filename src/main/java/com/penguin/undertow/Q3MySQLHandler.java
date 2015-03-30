package com.penguin.undertow;

import io.undertow.server.HttpServerExchange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;
import java.util.HashMap;


/**
 * Question 3 MySQL Handler.
 */

public class Q3MySQLHandler extends BaseHttpHandler {
  /**
   * Flag map
   */
  private static final HashMap<String, String> flagMap = new HashMap<String,String>();

  /**
   * Preload flag map
   */
  static {
	  flagMap.put("1", "-");
	  flagMap.put("2", "+");
	  flagMap.put("3", "*");
  }

  /**
   * Keys for http params
   */
  private static final String KEY_UID = "userid";

  /**
   * Connection, PreparedStatement and ResultSet
   */
  private Connection conn = null;
  private PreparedStatement pstmt = null;
  private ResultSet rs = null;

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
        + "' ORDER BY flag DESC, count DESC, retweetid ASC;";

    try {
      conn = ConnectionUtils.getInstance().getMySQLConnection();
      pstmt = conn.prepareStatement(sql);
      rs = pstmt.executeQuery();

      while (rs.next()) {
    	String relationship = flagMap.get(rs.getString("flag"));
        response = response.append(relationship)
        			.append(",")
        			.append(rs.getString("count"))
        			.append(",")
        			.append(rs.getString("retweetid"))
        			.append("\n");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    String result = response.toString();
    return getDefaultResponse() + result;
  }
}
