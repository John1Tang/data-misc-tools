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

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.storage.jdbc.conf.DatabaseType;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating the correct DatabaseAccessor class for the job
 */
public class TncDatabaseAccessorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(TncDatabaseAccessorFactory.class);

  private TncDatabaseAccessorFactory() {
  }


  public static DatabaseAccessor getAccessor(DatabaseType dbType) {

    DatabaseAccessor accessor = null;
    switch (dbType) {
    case MYSQL:
      accessor = new TncMySqlDatabaseAccessor();
      break;
    case JETHRO_DATA:
      accessor = new TncJethroDatabaseAccessor();
      break;

    case POSTGRES:
      accessor = new TncPostgresDatabaseAccessor();
      break;

    case ORACLE:
      accessor = new TncOracleDatabaseAccessor();
      break;

    case MSSQL:
      accessor = new TncMsSqlDatabaseAccessor();
      break;

    default:
      accessor = new TncGenericJdbcDatabaseAccessor();
      break;
    }

    return accessor;
  }


  public static DatabaseAccessor getAccessor(Configuration conf) {
    LOGGER.info("TncDatabaseAccessorFactory.getAccessor conf: {}", conf);
    DatabaseType dbType = DatabaseType.valueOf(conf.get(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()));
    return getAccessor(dbType);
  }

}
