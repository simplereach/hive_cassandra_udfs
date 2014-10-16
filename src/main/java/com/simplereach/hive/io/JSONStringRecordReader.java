package com.simplereach.hive.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;


public class JSONStringRecordReader extends LineRecordReader {
    private static final Log LOG = LogFactory.getLog(JSONStringRecordReader.class.getName());
    public JSONStringRecordReader(Configuration job, FileSplit inputSplit) throws IOException {
        super(job, inputSplit);
    }

    @Override
    public synchronized boolean next(LongWritable arg0, Text line) throws IOException {
        boolean res = super.next(arg0, line);

        if(line.getBytes().length <= 4){
          return false;
        }

        try {
            line.set(line.getBytes(), 0, line.getLength()-1);
            line.set(line.getBytes(), 1, line.getLength()-1);
            line.set(line.toString().replace("\\\"", "\"").getBytes());
            line.set(line.toString().replace("\\\\\"", "\\\\\\\"").getBytes());
            line.set(line.toString().replace("\\\\\\\\\\\\\\\\\\\"", "\"").getBytes());
        } catch (ArrayIndexOutOfBoundsException e) {
            LOG.error("ERROR IN NEXT", e);
            LOG.error("OFFENDING LINE" + line);
            return false;
        }


        return res;
    }

}