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
package com.thenetcircle.service.data.hive.jdbc.storagehandler;

import com.thenetcircle.service.data.hive.jdbc.storagehandler.dao.TncDatabaseAccessorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.storage.jdbc.JdbcInputSplit;
import org.apache.hive.storage.jdbc.JdbcRecordReader;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.JdbcRecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TncJdbcRecordReader extends JdbcRecordReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TncJdbcRecordReader.class);

    private DatabaseAccessor dbAccessor = null;
    private JdbcRecordIterator iterator = null;
    private JdbcInputSplit split = null;
    private JobConf conf = null;
    private int pos = 0;


    public TncJdbcRecordReader(JobConf conf, JdbcInputSplit split) {
        super(conf, split);
        LOGGER.trace("Initializing JdbcRecordReader");
        this.split = split;
        this.conf = conf;
    }


    @Override
    public boolean next(LongWritable key, MapWritable value) throws IOException {
        try {
            if (dbAccessor == null) {
                LOGGER.info("TncJdbcRecordReader.next check ");
                dbAccessor = TncDatabaseAccessorFactory.getAccessor(conf);
                setDbAccessor(dbAccessor);
                iterator = dbAccessor.getRecordIterator(conf, split.getLimit(), split.getOffset());
                setIterator(iterator);
            }
        } catch (Exception e) {
            LOGGER.error("An error occurred while reading the next record from DB.", e);
            return false;
        }
        return super.next(key, value);
    }

    @Override
    public LongWritable createKey() {
        return super.createKey();
    }

    @Override
    public MapWritable createValue() {
        return super.createValue();
    }

    @Override
    public long getPos() throws IOException {
        return super.getPos();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public float getProgress() throws IOException {
        return super.getProgress();
    }

    @Override
    public void setDbAccessor(DatabaseAccessor dbAccessor) {
        super.setDbAccessor(dbAccessor);
    }

    @Override
    public void setIterator(JdbcRecordIterator iterator) {
        super.setIterator(iterator);
    }
}
