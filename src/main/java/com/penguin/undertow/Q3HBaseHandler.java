package com.penguin.undertow;

import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Handler for Question 2 : JDBC Query
 */

public class Q3HBaseHandler extends BaseHttpHandler {

  public class Q3Object implements Comparable<Q3Object> {
    public int relationship;
    public int count;
    public long buddy;

    public Q3Object(int relationship, int count, long buddy) {
      this.relationship = relationship;
      this.count = count;
      this.buddy = buddy;
    }

    @Override
    public int compareTo(Q3Object other) {
      int i = Integer.compare(relationship, other.relationship);
      if (i != 0) {
        return i;
      }
      int j = Integer.compare(other.count, count);
      if (j != 0) {
        return j;
      }
      return Long.compare(other.buddy, buddy);
    }
  }

  /**
   * Keys for http params
   */
  private static final String KEY_UID = "userid";

  /**
   * HBASE column family
   */
  private static final byte[] INFO_FAMILY = Bytes.toBytes("info");
  private static final byte[] QUALIFIER_BUDDY = Bytes.toBytes("buddy");
  private static final byte[] QUALIFIER_COUNT = Bytes.toBytes("count");
  private static final byte[] QUALIFIER_TYPE = Bytes.toBytes("type");

  private static final String TABLE_NAME = "twitter3";

  private static final HashMap<Integer, String> FLAG_MAP = new HashMap<Integer, String>();
  static {
    FLAG_MAP.put(1, "-");
    FLAG_MAP.put(2, "+");
    FLAG_MAP.put(3, "*");
  }

  @Override
  public String getResponse(HttpServerExchange exchange) {
    List<Q3Object> objects = new ArrayList<Q3Object>();
    // Get parameters
    Deque<String> uid = exchange.getQueryParameters().get(KEY_UID);

    if (uid == null || uid.isEmpty()) {
      return getDefaultResponse();
    }

    // String userId = uid.peekFirst();
    String userId = "1000334484";
    print("userid: " + userId);
    print(Arrays.toString(Bytes.toBytes(userId)));

    if (userId == null) {
      return getDefaultResponse();
    }

    Scan scan = new Scan();
    RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(
        Bytes.toBytes(userId)));
    scan.setFilter(rowFilter);

    print("scan: " + scan.toString());

    // Get response
    ResultScanner scanner = null;
    HTableInterface table = null;

    try {
      table = ConnectionUtils.getInstance().getHTable(TABLE_NAME);
      scanner = table.getScanner(scan);
      print("scanner: " + scanner.toString());
    } catch (IOException e) {
      System.out.println("EXCEPTION " + e.toString());
    } /*finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      }*/

    StringBuilder response = new StringBuilder();

    for (Result result : scanner) {
      for (KeyValue keyValue : result.list()) {
        System.out.println("Qualifier : " + keyValue.getKeyString()
            + " : Value : " + Bytes.toString(keyValue.getValue()));
      }
      print("done printing");

      int flag = Integer.parseInt(getValue(result, QUALIFIER_TYPE));
      if (flag != 3) {
        flag = 3 - flag;
      }
      // String relationship = FLAG_MAP.get(flag);
      long buddy = Long.parseLong(getValue(result, QUALIFIER_BUDDY));
      int count = Integer.parseInt(getValue(result, QUALIFIER_COUNT));

      objects.add(new Q3Object(flag, count, buddy));
    }
    print("done adding objects: " + objects.size());

    Collections.sort(objects);
    int total = objects.size();
    for (int i = 0; i < total; i++) {
      Q3Object o = objects.get(i);
      print("=====");
      print(o.toString());
      response = response.append(FLAG_MAP.get(o.relationship)).append(",")
          .append(o.count).append(",").append(o.buddy).append("\n");
    }

    return response.toString();
  }

  private String getValue(Result result, byte[] qualifier) {
    byte[] value = result.getValue(INFO_FAMILY, qualifier);
    if (value == null || value.length == 0) {
      return null;
    }
    String res = null;
    try {
      res = new String(value, "UTF-8");

      res = res.replaceAll("pngn134", "\n");
    } catch (UnsupportedEncodingException e) {
      System.out.println("EXCEPTION " + e.toString());
    }

    return res;
  }

  protected void print(String print) {
    System.out.println(print);
  }
}
