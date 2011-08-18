package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * and counts them. The output is a locally sorted list of words and the 
 * count of how often they occurred.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar wordcount
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 */
public class WordCountText2Rc extends Configured implements Tool {
  
  /**
   * Counts the words in each line.
   * For each line of input, break the line into words and emit them as
   * (<b>word</b>, <b>1</b>).
   */
  public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, /*IntWritable*/BytesRefArrayWritable> {
    
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.WordCountText2Rc");
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
//@Override
    public void map(LongWritable key, Text value, 
                    OutputCollector<Text, /*IntWritable*/BytesRefArrayWritable> output, 
                    Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      BytesRefArrayWritable lineBytes = new BytesRefArrayWritable();
      int index = 0;
      while (itr.hasMoreTokens()) {
        String wordS = itr.nextToken();
        byte[] wordBytes = wordS.getBytes("UTF-8");
        BytesRefWritable wordBytesWritable = new BytesRefWritable(wordBytes, 0,
            wordBytes.length);
        lineBytes.set(index++, wordBytesWritable);
        LOG.info("### MapKey "+key.toString() + " | Index "+index+" | wordS "+wordS );
      }

      output.collect(word, lineBytes);
    }
  }
  
//  /**
//   * A reducer class that just emits the sum of the input values.
//   */
//  public static class Reduce extends MapReduceBase
//    implements Reducer<Text, IntWritable, Text, IntWritable> {
//    
//    public void reduce(Text key, Iterator<IntWritable> values,
//                       OutputCollector<Text, IntWritable> output, 
//                       Reporter reporter) throws IOException {
//      int sum = 0;
//      while (values.hasNext()) {
//        sum += values.next().get();
//      }
//      output.collect(key, new IntWritable(sum));
//    }
//  }
  
  static int printUsage() {
    System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), WordCountText2Rc.class);
    conf.setJobName("wordcount_text2rc");
 
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(BytesRefArrayWritable.class);
    
    conf.setMapperClass(MapClass.class);        
//    conf.setCombinerClass(Reduce.class);
//    conf.setReducerClass(Reduce.class);
    
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(RCFileOutputFormat.class);
    RCFileOutputFormat.setColumnNumber(conf, 2);
    
    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
                         other_args.size() + " instead of 2.");
      return printUsage();
    }
    FileInputFormat.setInputPaths(conf, other_args.get(0));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
        
    JobClient.runJob(conf);
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WordCountText2Rc(), args);
    System.exit(res);
  }

}
