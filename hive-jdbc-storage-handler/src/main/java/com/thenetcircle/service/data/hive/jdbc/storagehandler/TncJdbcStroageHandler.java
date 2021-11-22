package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.JarUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hive.storage.jdbc.JdbcStorageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TncJdbcStroageHandler extends JdbcStorageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TncJdbcStroageHandler.class);

    private Configuration conf;


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }


    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return TncJdbcSerDe.class;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return TncJdbcInputFmt.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return TncJdbcOutputFmt.class;
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        List<Class<?>> classesToLoad = new ArrayList<>();
        classesToLoad.add(org.apache.hive.storage.jdbc.JdbcInputSplit.class);
        classesToLoad.add(org.apache.commons.dbcp.BasicDataSourceFactory.class);
        classesToLoad.add(org.apache.commons.pool.impl.GenericObjectPool.class);
        // Adding mysql jdbc driver if exists
        try {
            classesToLoad.add(Class.forName("com.mysql.jdbc.Driver"));
        } catch (Exception e) {
        }

        // support mysql8 connector
        try {
            classesToLoad.add(Class.forName("com.mysql.cj.jdbc.Driver"));
        } catch (Exception e) {
        }

        // Adding postgres jdbc driver if exists
        try {
            classesToLoad.add(Class.forName("org.postgresql.Driver"));
        } catch (Exception e) {
        } // Adding oracle jdbc driver if exists
        try {
            classesToLoad.add(Class.forName("oracle.jdbc.OracleDriver"));
        } catch (Exception e) {
        } // Adding mssql jdbc driver if exists
        try {
            classesToLoad.add(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"));
        } catch (Exception e) {
        }
        try {
            JarUtils.addDependencyJars(conf, classesToLoad);
        } catch (IOException e) {
            LOGGER.error("Could not add necessary JDBC storage handler dependencies to classpath", e);
        }
    }
}
