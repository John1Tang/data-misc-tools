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

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.dao.GenericJdbcDatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.JdbcRecordIterator;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 */
public class TncGenericJdbcDatabaseAccessor extends GenericJdbcDatabaseAccessor {

  protected static final String DBCP_CONFIG_PREFIX = JdbcStorageConfigManager.CONFIG_PREFIX + ".dbcp";
  protected static final Logger LOGGER = LoggerFactory.getLogger(TncGenericJdbcDatabaseAccessor.class);
  protected DataSource dbcpDataSource = null;


  public TncGenericJdbcDatabaseAccessor() {

  }


  @Override
  public List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String metadataQuery = getMetaDataQuery(conf);
      LOGGER.debug("Query to execute is [{}]", metadataQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(metadataQuery);
      rs = ps.executeQuery();

      ResultSetMetaData metadata = rs.getMetaData();
      int numColumns = metadata.getColumnCount();
      List<String> columnNames = new ArrayList<String>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        // use what SQL AS clause shows
        columnNames.add(metadata.getColumnLabel(i + 1));
      }

      return columnNames;
    }
    catch (Exception e) {
      LOGGER.error("Error while trying to get column names.", e);
      throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e.getMessage(), e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }

  }


  @Override
  public JdbcRecordIterator
    getRecordIterator(Configuration conf, int limit, int offset) throws HiveJdbcDatabaseAccessException {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String limitQuery = addLimitAndOffsetToQuery(sql, limit, offset);
      LOGGER.info("Query to execute is [{}]", limitQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(limitQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      ps.setFetchSize(getFetchSize(conf));
      rs = ps.executeQuery();

      return new TncJdbcRecordIterator(conn, ps, rs, conf.get(serdeConstants.LIST_COLUMN_TYPES));
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query", e);
    }
  }

  protected void initializeDatabaseConnection(Configuration conf) throws Exception {
    if (dbcpDataSource == null) {
      synchronized (this) {
        if (dbcpDataSource == null) {
          Properties props = getConnectionPoolProperties(conf);
          dbcpDataSource = BasicDataSourceFactory.createDataSource(props);
        }
      }
    }
  }

  @Override
  public int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException {
    return super.getTotalNumberOfRecords(conf);
  }

  @Override
  protected String getMetaDataQuery(Configuration conf) {
    return super.getMetaDataQuery(conf);
  }

  @Override
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    return super.addLimitAndOffsetToQuery(sql, limit, offset);
  }

  @Override
  protected String addLimitToQuery(String sql, int limit) {
    return super.addLimitToQuery(sql, limit);
  }

  @Override
  protected void cleanupResources(Connection conn, PreparedStatement ps, ResultSet rs) {
    super.cleanupResources(conn, ps, rs);
  }

  @Override
  protected Properties getConnectionPoolProperties(Configuration conf) throws Exception {
    return super.getConnectionPoolProperties(conf);
  }

  @Override
  protected Properties getDefaultDBCPProperties() {
    return super.getDefaultDBCPProperties();
  }

  @Override
  protected int getFetchSize(Configuration conf) {
    return super.getFetchSize(conf);
  }
}
