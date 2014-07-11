package com.simplereach.hive.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * A custom input format for dealing with JSON Strings
 *
 */
class JSONStringInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf job, Reporter reporter) throws IOException {
        return new JSONStringRecordReader(job, (FileSplit)inputSplit);
    }
}

