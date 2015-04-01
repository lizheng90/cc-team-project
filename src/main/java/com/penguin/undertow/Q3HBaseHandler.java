package com.penguin.undertow;

import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;

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

  private static final HashMap<String, String> FLAG_MAP = new HashMap<String, String>();
  static {
    FLAG_MAP.put("1", "+");
    FLAG_MAP.put("2", "-");
    FLAG_MAP.put("3", "*");
  }

  @Override
  public String getResponse(HttpServerExchange exchange) {
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

      String flag = getValue(result, QUALIFIER_TYPE);
      String relationship = FLAG_MAP.get(flag);
      String buddy = getValue(result, QUALIFIER_BUDDY);
      String count = getValue(result, QUALIFIER_COUNT);

      response = response.append(relationship).append(",").append(count)
          .append(",").append(buddy).append("\n");
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
