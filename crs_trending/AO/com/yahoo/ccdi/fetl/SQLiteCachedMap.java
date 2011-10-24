package com.yahoo.ccdi.fetl;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.hadoop.record.Buffer;

public class SQLiteCachedMap {
  private HashMap<Buffer, Buffer> cachedMap = null;
  private HashMap<Buffer, Buffer> cachedValueMap = null;
  private int numCachedEntries = 0;
  private Connection dbConn = null;
  private PreparedStatement dbPrep = null;

  public SQLiteCachedMap(String dbFilePath, int entries)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.sqlite.JDBC");

    cachedMap = new HashMap<Buffer, Buffer>();
    cachedValueMap = new HashMap<Buffer, Buffer>();

    numCachedEntries = entries;

    dbConn = DriverManager.getConnection("jdbc:sqlite:" + dbFilePath);
    dbPrep = dbConn.prepareStatement("select value from pty_map where key=?;");
  }

  private Buffer insertIntoCache(Buffer key, Buffer value) {
    if (numCachedEntries != 0) {
      numCachedEntries--;

      // check if value has been in the value cache
      Buffer cachedValue = cachedValueMap.get(value);

      if (cachedValue == null) {
        cachedValue = value;
        cachedValueMap.put(cachedValue, cachedValue);
      }

      cachedMap.put(key, cachedValue);

      return cachedValue;
    }

    return value;
  }

  private Buffer getFromSqlite(Buffer key) {
    try {
      dbPrep.setString(1, key.toString("UTF-8"));
      ResultSet rs = dbPrep.executeQuery();

      if (rs.next()) {
        return new Buffer(rs.getString(1).getBytes());
      }

      rs.close();
    } catch ( UnsupportedEncodingException e ) {
      e.printStackTrace();
      return null;
    } catch ( SQLException e ) {
      e.printStackTrace();
      return null;
    }

    return null;
  }

  public String lookup(String keyString) throws UnsupportedEncodingException,
      SQLException {
    Buffer keyBuff = new Buffer(keyString.getBytes());
    if (keyBuff == null || keyBuff.getCount() == 0) {
      return null;
    }
    Buffer valueBuff = cachedMap.get(keyBuff);

    if (valueBuff != null) {
      return valueBuff.toString("UTF-8");
    }

    // check sqlite DB
    valueBuff = getFromSqlite(keyBuff);

    if (valueBuff != null) {
      return insertIntoCache(keyBuff, valueBuff).toString("UTF-8");
    }

    return null;
  }

  public void closeDBConnection() throws SQLException {
    dbConn.close();
  }
}
