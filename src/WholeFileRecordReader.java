/*
####################################################################################
Stephen W. Thomas
sthomas@cs.queensu.ca
Queen's University

WholeFileRecordReader.java

This class overrides the default Hadoop behaviour of reading records one line at
a time. Instead, it reads files in their entirety. 

####################################################################################
*/ 


package ca.queensu.sail.doofuslarge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;


class WholeFileRecordReader extends RecordReader<NullWritable, Text> {
    private FileSplit fileSplit;
    private Configuration conf;
    private Text value = new Text();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
    }


    /* This is the workhorse method. What we need to do is the following.
    - If we haven't seen this file before, read the entire file, return in, and set a
      flag (i.e., processed) that we've already seen the file.
    - If we have already seen this file, then don't do anything, because we've
      already read it and returned it.*/
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                // Read the entire file at once
                IOUtils.readFully(in, contents, 0, contents.length);
                value.set(contents, 0, contents.length);
            } finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }


    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Text getCurrentValue() throws IOException,
    InterruptedException {
        return value;
    }
    @Override
    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        //nothing to do here, since we've already read the whole file
    }
}



