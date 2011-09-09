package com.yahoo.ccdi.fetl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class ABF1SEQPrinter {

  /**
   * A reducer class that just emits the sum of the input values.
   */

  public static class Reduce extends MapReduceBase
    implements Reducer<ETLKey, LOETLValue, ETLKey, LOETLValue> {
    
    public void reduce(ETLKey key, Iterator<LOETLValue> values,
                       OutputCollector<ETLKey, LOETLValue> output, 
                       Reporter reporter) throws IOException {
      while (values.hasNext()) {
      output.collect(key, values.next());
      }
    }
  }

  public static class NonSplitableSequenceFileInputFormat 
  extends SequenceFileInputFormat {
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }
}
  
  public static void main(String[] args) throws Exception {        
      Configuration conf = new Configuration();
      
      String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
  
      JobConf ABF1Print = new JobConf(ABF1Printer.class);
      
      ABF1Print.setJobName(conf.get("mapred.job.name", "ETLSeq2ETLNOKEYSeq"));
      ABF1Print.setMapperClass(ABF1SEQPrinterMapper.class);
      ABF1Print.setReducerClass(Reducer.class);
      
      //ABF1Print.setInputFormat(NonSplitableSequenceFileInputFormat.class);
      ABF1Print.setInputFormat(SequenceFileInputFormat.class);
      ABF1Print.setOutputFormat(SequenceFileOutputFormat.class);
      
      ABF1Print.setMapOutputKeyClass(ETLKey.class);
      ABF1Print.setMapOutputValueClass(LOETLValue.class);
      
      ABF1Print.setOutputKeyClass(ETLKey.class);
      ABF1Print.setOutputValueClass(LOETLValue.class);
      
      ABF1Print.setCompressMapOutput(true);
      
      ABF1Print.set("mapred.job.queue.name", conf.get("mapred.job.queue.name"));
      ABF1Print.set("mapred.job.queue.name", "audience_fetl");

      // compress
      ABF1Print.set("mapred.output.compress", "true");
      ABF1Print.set("mapred.output.compression.type", "BLOCK");
      ABF1Print.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
      
      // new style of processing arguments
      List<String> other_args = new ArrayList<String>();
      for(int i=0; i < args.length; ++i) {

        try {
          if ("-m".equals(args[i])) {
            ABF1Print.setNumMapTasks(Integer.parseInt(args[++i]));
          } else if ("-r".equals(args[i])) {
            ABF1Print.setNumReduceTasks(Integer.parseInt(args[++i]));
          } else if ("-cblock".equals(args[i])) {
            //io.seqfile.compress.blocksize
            ABF1Print.setInt("io.seqfile.compress.blocksize", Integer.parseInt(args[++i])); // 32 M
          } else if ("-raw".equals(args[i])) {
            //aBF1RCPrint.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, Integer.parseInt(args[++i])); //32 M
            ABF1Print.setInt("hive.io.rcfile.record.buffer.size", Integer.parseInt(args[++i]));
          } else if ("-splitsize".equals(args[i])) {
            ABF1Print.setInt("mapred.min.split.size", Integer.parseInt(args[++i])); // 32 M
          } 
          else {
            // by default, 0 reducer
            ABF1Print.setNumReduceTasks(0);
            other_args.add(args[i]);
          }
        } catch (NumberFormatException except) {
          System.out.println("ERROR: Integer expected instead of " + args[i]);
          return;
        } catch (ArrayIndexOutOfBoundsException except) {
          System.out.println("ERROR: Required parameter missing from " +
                             args[i-1]);
          return;
        }
      }
      FileInputFormat.setInputPaths(ABF1Print, other_args.get(0));
      FileOutputFormat.setOutputPath(ABF1Print, new Path(other_args.get(1)));

      RunningJob myPrinter = JobClient.runJob(ABF1Print);
  }
}
