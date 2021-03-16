package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import com.thenetcircle.service.data.hive.jdbc.storagehandler.dao.TncDatabaseAccessorFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.storage.jdbc.JdbcInputFormat;
import org.apache.hive.storage.jdbc.JdbcInputSplit;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TncJdbcInputFmt extends JdbcInputFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(TncJdbcInputFmt.class);
    private DatabaseAccessor dbAccessor = null;

    public TncJdbcInputFmt() {
    }

    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        if (!(split instanceof JdbcInputSplit)) {
            throw new RuntimeException("Incompatible split type " + split.getClass().getName() + ".");
        }

        return new TncJdbcRecordReader(job, (JdbcInputSplit) split);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        if (dbAccessor == null) {
            dbAccessor = TncDatabaseAccessorFactory.getAccessor(job);
            super.setDbAccessor(dbAccessor);
        }
        return super.getSplits(job, numSplits);
    }
}
