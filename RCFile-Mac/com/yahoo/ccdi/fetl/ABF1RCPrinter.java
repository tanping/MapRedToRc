package com.yahoo.ccdi.fetl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class ABF1RCPrinter {
  
  public static final int COLUMN_NUMBER = 38;
  
  /**
   * A reducer class that just emits the sum of the input values.
   */

  public static class Reduce extends MapReduceBase
    implements Reducer<LongWritable, LOKeyValue, LongWritable, LOKeyValue> {
    
    public void reduce(LongWritable key, Iterator<LOKeyValue> values,
                       OutputCollector<LongWritable, LOKeyValue> output, 
                       Reporter reporter) throws IOException {
      while (values.hasNext()) {
      output.collect(key, values.next());
      }
    }
  }

    public static void main(String[] args) throws Exception {        
        Configuration conf = new Configuration();
            
        JobConf aBF1RCPrint = new JobConf(ABF1RCPrinter.class);
        
        aBF1RCPrint.setJobName(conf.get("mapred.job.name", "abf1_rc_print"));
        aBF1RCPrint.setMapperClass(ABF1RCPrinterMapper.class);
        aBF1RCPrint.setReducerClass(/*IdentityReducer.class*/Reducer.class);
        
        aBF1RCPrint.setInputFormat(SequenceFileInputFormat.class);
        aBF1RCPrint.setOutputFormat(RCFileOutputFormat.class);
        RCFileOutputFormat.setCompressOutput(aBF1RCPrint, true);
        RCFileOutputFormat.setColumnNumber(aBF1RCPrint, COLUMN_NUMBER);
        aBF1RCPrint.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, 128 * 1024 * 1024); //128 M
        //aBF1RCPrint.setInt("hive.io.rcfile.record.buffer.size", 128 * 1024 * 1024);
        
        aBF1RCPrint.setMapOutputKeyClass(LongWritable.class);
        aBF1RCPrint.setMapOutputValueClass(LOKeyValue.class);
        
        aBF1RCPrint.setOutputKeyClass(LongWritable.class);
        aBF1RCPrint.setOutputValueClass(LOKeyValue.class);
        
        aBF1RCPrint.setCompressMapOutput(true);
        
        aBF1RCPrint.set("mapred.job.queue.name", "audience_fetl");
        
        // set compression
        aBF1RCPrint.set("mapred.output.compress", "true");
        aBF1RCPrint.set("mapred.output.compression.type", "BLOCK");
        aBF1RCPrint.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        
        // old style of processing arguments
//        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
//        if(otherArgs.length != 3) {
//            aBF1RCPrint.setNumReduceTasks(0);
//        } else {
//            System.out.println(otherArgs[2]);
//            aBF1RCPrint.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
//        }
//      FileInputFormat.setInputPaths(aBF1RCPrint, otherArgs[0]);
//      FileOutputFormat.setOutputPath(aBF1RCPrint, new Path(otherArgs[1]));
        
        // new style of processing arguments
        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {

          try {
            if ("-m".equals(args[i])) {
              aBF1RCPrint.setNumMapTasks(Integer.parseInt(args[++i]));
            } else if ("-r".equals(args[i])) {
              aBF1RCPrint.setNumReduceTasks(Integer.parseInt(args[++i]));
            } else {
              // by default, 0 reducer
              aBF1RCPrint.setNumReduceTasks(0);
              other_args.add(args[i]);
            }
          } catch (NumberFormatException except) {
            System.out.println("ERROR: Integer expected instead of " + args[i]);
            printUsage();
            return;
          } catch (ArrayIndexOutOfBoundsException except) {
            System.out.println("ERROR: Required parameter missing from " +
                               args[i-1]);
            printUsage();
            return;
          }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
          System.out.println("ERROR: Wrong number of parameters: " +
                             other_args.size() + " instead of 2.");
          printUsage();
          return;
        }
        FileInputFormat.setInputPaths(aBF1RCPrint, other_args.get(0));
        FileOutputFormat.setOutputPath(aBF1RCPrint, new Path(other_args.get(1)));
  
        RunningJob myPrinter = JobClient.runJob(aBF1RCPrint);
    }
    
    static void printUsage() {
      System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
      ToolRunner.printGenericCommandUsage(System.out);
    }
}

