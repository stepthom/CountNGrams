/*
####################################################################################
Stephen W. Thomas
sthomas@cs.queensu.ca
Queen's University

CountNGrams.java

This class uses Hadoop to count the number of occurances of n-grams in a given set of text
files.

An n-gram is a list of n words, which are adjacent to each other in a text file. 
For example, if the text file contained the text "one two three", there would be
three 3 1-grams ("one", "two", "three"), 2 2-grams ("one_two", "two_three"), and
1 3-gram ("one_two_three").

Input files need to be not split, as the entire file needs to be given as a
value to the mapper (as opposed to being given just one line at a time, the
default behaviour in Hadoop). This is so we can accurately count n-grams that may
span across a line in the file.  To do this, we use a custom FileInputFormat,
which in turn uses a custom FileRecordReader. Both of these classes are included
in this package.

This class has been tested against hadoop 1.0.3. 

N-grams of sizes 1, 2, 3, 4, and 5 are supported. You can specify on the command
line which you want to include via the third parameter. For example, the
command:

countngrams input output 1,2,3,5

will computer counts for ngrams of size 1, 2, 3 and 5. These are passed from the
main() method to the map() method via Hadoop configuration parameters. 
####################################################################################
*/ 

package ca.queensu.sail.doofuslarge;

import ca.queensu.sail.doofuslarge.WholeFileInputFormat;
import ca.queensu.sail.doofuslarge.WholeFileRecordReader;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CountNGrams {

  /* This mapper takes as input an entire file, and emits
  <n-gram, 1> pairs for all n-grams found, for n=1..5. */
  public static class MyMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text gram1 = new Text();
    private Text gram2 = new Text();
    private Text gram3 = new Text();
    private Text gram4 = new Text();
    private Text gram5 = new Text();

    private boolean doGram1, doGram2, doGram3, doGram4, doGram5;
      
    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {


      // Get configuration parameters, i.e., which n-grams to compute
      Configuration conf = context.getConfiguration();
      doGram1 = Boolean.valueOf(conf.get("gram1"));
      doGram2 = Boolean.valueOf(conf.get("gram2"));
      doGram3 = Boolean.valueOf(conf.get("gram3"));
      doGram4 = Boolean.valueOf(conf.get("gram4"));
      doGram5 = Boolean.valueOf(conf.get("gram5"));

      // Value should have the entire file in it, including newlines, so split
      // based on all whitespace.

      // TODO: Need to implement more sophisticated splitting and cleaning;
      // maybe use my lcsp tool?

      String words[];
      words = value.toString().split("\\s+");

      for (int i=0; i<words.length; ++i){

        // TODO: can easily make this a loop to support arbitrary n; however,
        // who wants/needs arbitrary n?

        // Now, output the n-grams!
        if (doGram1){
            gram1.set(words[i]);
            context.write(gram1, one);
        }
        if (doGram2 && i < (words.length-1)){
            gram2.set(words[i] + "_" + words[i+1]);
            context.write(gram2, one);
        }
        if (doGram3 && i < (words.length-2)){
            gram3.set(words[i] + "_" + words[i+1] + "_" + words[i+2]);
            context.write(gram3, one);
        }
        if (doGram4 && i < (words.length-3)){
            gram4.set(words[i] + "_" + words[i+1] + "_" + words[i+2] + "_" + words[i+3]);
            context.write(gram4, one);
        }
        if (doGram5 && i < (words.length-4)){
            gram5.set(words[i] + "_" + words[i+1] + "_" + words[i+2] 
                + "_" + words[i+3] + "_" + words[i+4]);
            context.write(gram5, one);
        }
      }
    }
  }

 
  /* This reducer takes as input the <n-gram, 1> pairs (which were emitted
  from the mapper) and simply sums the number of times is saw each n-gram. */ 
  public static class MyReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);

      // TODO: how to get map to sort output by value (numerically), instead of key?
      //String keyWithNumber = String.format("%05d:%s", sum, keyStr);
      //key.set(keyWithNumber);
      context.write(key, result);
    }
  }


  /* The main method just ties it all together */ 
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: countngrams input_dir output_dir 1,2,3,4,5?");
      System.err.println("E.g: countngrams input_dir output_dir 1,4,5");
      System.exit(-1);
    }

    // Read the third argument to see which ngrams we should be looking for.
    // strore the results in a "grami" configuration parameter, as true or false
    // The Mapper can access these later.
    String ngramOptions = otherArgs[2];
    for (int i = 1; i < 5; ++i){
        if (ngramOptions.contains(Integer.toString(i))){
            conf.set("gram"+i, "true");
        } else {
            conf.set("gram"+i, "false");
        }
    }
    
    Job job = new Job(conf, "Count NGrams");

    job.setJarByClass(CountNGrams.class);
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setInputFormatClass(WholeFileInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}

