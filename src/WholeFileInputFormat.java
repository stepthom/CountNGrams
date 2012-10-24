/*
####################################################################################
Stephen W. Thomas
sthomas@cs.queensu.ca
Queen's University

WholeFileInputFormat.java

This class creates a custom record reader class, namely the custom reader in
this package: ca.queensu.sail.doofuslarge.WholeFileRecordReader. 

To see an example usage, see CountNGrams.java.

####################################################################################
*/ 

package ca.queensu.sail.doofuslarge;

import ca.queensu.sail.doofuslarge.WholeFileRecordReader;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;


public class WholeFileInputFormat
    extends FileInputFormat<NullWritable, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    /* Here, we just need to create an instance of our custom record reader (see
    WholeFileRecordReader.java), and return it. */
    @Override
    public RecordReader<NullWritable, Text> 
        createRecordReader(InputSplit split, TaskAttemptContext context) 
        throws IOException, InterruptedException {
    
        WholeFileRecordReader reader = new WholeFileRecordReader();
        reader.initialize(split, context);
        return reader;
    }
}


