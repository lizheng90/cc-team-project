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

  /**
   * Connection, PreparedStatement and ResultSet
   */
  private Connection conn = null;
  private PreparedStatement pstmt = null;
  private ResultSet rs = null;

  @Override
  public String getResponse(HttpServerExchange exchange) {
    // Get parameters
    Deque<String> hashtag = exchange.getQueryParameters().get(KEY_HASHTAG);
    Deque<String> start = exchange.getQueryParameters().get(KEY_START);
    Deque<String> end = exchange.getQueryParameters().get(KEY_END);

    String tag = hashtag.peekFirst();
    String starttime = start.peekFirst();
    starttime = starttime.replaceAll("-", "") + "000000";
    String endtime = end.peekFirst();
    endtime = endtime.replaceAll("-", "") + "235959";

    StringBuilder response = new StringBuilder();
    String sql = "SELECT * FROM twitter4 WHERE tag='" + tag
        + "' AND time BETWEEN '" + starttime + "' AND '" + endtime + "' ORDER BY tweetid ASC;";

    try {
      conn = ConnectionUtils.getInstance().getMySQLConnection();
      pstmt = conn.prepareStatement(sql);
      rs = pstmt.executeQuery();

      while (rs.next()) {
    	String thisTime = getTime(rs.getString("time"));
        response = response.append(rs.getString("tweetid"))
        			.append(",")
        			.append(rs.getString("userid"))
        			.append(",")
        			.append(thisTime);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    String result = response.toString();
    return getDefaultResponse() + result;
  }

  public String getTime(String str) {
	  String result =  str.substring(0,4) + "-" + str.substring(4,6) + "-" + str.substring(6,8) + "+"
			  			+ str.substring(8,10) + ":" + str.substring(10,12) + ":" + str.substring(12,14) + "\n";
	  return result;
  }
}
