/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.thenetcircle.service.data.hive.jdbc.storagehandler.dao;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.storage.jdbc.dao.JdbcRecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An iterator that allows iterating through a SQL resultset. Includes methods to clear up resources.
 */
public class TncJdbcRecordIterator extends JdbcRecordIterator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TncJdbcRecordIterator.class);

  private Connection conn;
  private PreparedStatement ps;
  private ResultSet rs;
  private ArrayList<TypeInfo> columnTypes = null;

  public TncJdbcRecordIterator(Connection conn, PreparedStatement ps, ResultSet rs, String typeString) {
    super(conn, ps, rs, typeString);
    this.conn = conn;
    this.ps = ps;
    this.rs = rs;
    if (typeString != null) {
      this.columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(typeString);
    }
  }

  @Override
  public Map<String, Object> next() {
    try {
      LOGGER.info("TncJdbcRecordIterator.next start...");
      ResultSetMetaData metadata = rs.getMetaData();
      int numColumns = metadata.getColumnCount();
      Map<String, Object> record = new HashMap<String, Object>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        // use what the SQL AS clause shows
        String key = metadata.getColumnLabel(i + 1);
        Object value;
        if (columnTypes!=null && columnTypes.get(i) instanceof PrimitiveTypeInfo) {
          // This is not a complete list, barely make information schema work
          switch (((PrimitiveTypeInfo)columnTypes.get(i)).getTypeName()) {
          case "int":
          case "smallint":
          case "tinyint":
            value = rs.getInt(i + 1);
            break;
          case "bigint":
            value = rs.getLong(i + 1);
            break;
          case "float":
            value = rs.getFloat(i + 1);
            break;
          case "double":
            value = rs.getDouble(i + 1);
            break;
          case "bigdecimal":
            value = HiveDecimal.create(rs.getBigDecimal(i + 1));
            break;
          case "boolean":
            value = rs.getBoolean(i + 1);
            break;
          case "string":
          case "char":
          case "varchar":
            value = rs.getString(i + 1);
            break;
          case "datetime":
            value = rs.getDate(i + 1);
            break;
          case "timestamp":
            value = rs.getTimestamp(i + 1);
            break;
          default:
            value = rs.getObject(i + 1);
            break;
          }
        } else {
          value = rs.getObject(i + 1);
        }
        // if column name repeats
        if (record.containsKey(key)) {
          key = key + "_1";
        }
        record.put(key, value);
      }
      LOGGER.info("TncJdbcRecordIterator.next end...");
      LOGGER.info("TncJdbcRecordIterator.next data: {}", record);
      return record;
    }
    catch (Exception e) {
      LOGGER.warn("next() threw exception", e);
      return null;
    }
  }

  @Override
  public void close() {
    super.close();
    try {
      rs.close();
      ps.close();
      conn.close();
    }
    catch (Exception e) {
      LOGGER.warn("Caught exception while trying to close database objects", e);
    }
  }

  @Override
  public boolean hasNext() {
    return super.hasNext();
  }

  @Override
  public void remove() {
    super.remove();
  }

}
