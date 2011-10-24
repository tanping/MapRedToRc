package com.yahoo.ccdi.fetl.ao.cookie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yahoo.ccdi.fetl.GlobalConfVarNames;
import com.yahoo.ccdi.fetl.MetricsTypeTextOutputFormat;
import com.yahoo.ccdi.fetl.SequenceProjectorFormatExt;
import com.yahoo.ccdi.fetl.sequence.mapreduce.SequenceProjectorFormat;

public class CookieCounterDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String queueName = conf.get("mapred.job.queue.name");
    if (queueName == null || queueName.isEmpty()) {
      conf.set("mapred.job.queue.name", "audience_felt");
    } else {
      conf.set("mapred.job.queue.name", queueName);
    }
    
    Job job = new Job(conf);
    job.setJarByClass(CookieCounterDriver.class);
    job.setJobName(conf.get("mapred.job.name", "crs_trending_cookie_counter"));
    
   String projFields = conf.get("projectedfields");
   if ( projFields == null || projFields.equals("") ) {
     System.err.println("Must provide fields to be projected.");
     return -1;
   }  
   
   //projFields = "bcookie:String";
    SequenceProjectorFormat
        .setProjections(
            job,
            "",
            projFields);

    job.setInputFormatClass(SequenceProjectorFormatExt.class);
    //job.setOutputFormatClass(MetricsTypeTextOutputFormat.class);
    job.setMapperClass(CookieCounterMapper.class);
    // we need a combiner even if there is no reducer. Otherwise, it is really slow.
    job.setCombinerClass(IntSumCombiner.class);
    //job.setCombinerClass(org.apache.hadoop.mapreduce.Reducer.class);
    job.setReducerClass(IntSumReducer.class);
    //job.setReducerClass(DummyReducer.class);
    //job.setReducerClass(org.apache.hadoop.mapreduce.Reducer.class);
    job.setNumReduceTasks(512);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    String input = conf.get("input");
    if (input == null || input.isEmpty()) {
      String err = "Please provide input folder using syntax -Dinput=.. ";
      throw new RuntimeException(err);
    }
    FileInputFormat.setInputPaths(job, new Path(input));

    String output = conf.get("output");
    if (output == null || output.isEmpty()) {
      String err = "Please provide output folder using syntax -Doutput=.. ";
      System.err.println(err);
      throw new RuntimeException(err);
    }
    FileOutputFormat.setOutputPath(job, new Path(output));

    boolean success = job.waitForCompletion(true);
   
    if (job.isSuccessful()) {
      FileSystem hdfs = FileSystem.get(conf);
      FSDataOutputStream outputStream = null;
      System.out.println("before creating the file");
      outputStream = hdfs.create(
          new Path(output + GlobalConfVarNames.ALL_DIR+"Metrics"), true);
      System.out.println("output is : " + output + GlobalConfVarNames.ALL_DIR
          + "Metrics");
      String propArray[] = {GlobalConfVarNames.ALL_PROPERTY_ID,
          GlobalConfVarNames.MAIL_PROPERTY_ID,
          GlobalConfVarNames.FRONT_PAGE_PROPERTY_ID,
          GlobalConfVarNames.NEWS_PROPERTY_ID,
          GlobalConfVarNames.FINANCE_PROPERTY_ID,
          GlobalConfVarNames.SEARCH_PROPERTY_ID,
          GlobalConfVarNames.SPORTS_PROPERTY_ID,
          GlobalConfVarNames.OMG_PROPERTY_ID
          };
      
      StringBuilder sb = new StringBuilder();
      for (String p : propArray) {
        sb.append(
          p +"\tPAGE_VIEWS\t" + 
          job.getCounters().findCounter(p,GlobalConfVarNames.COUNTER_PV).getValue()
          + "\tAD_CLICKS\t" + 
          job.getCounters().findCounter(p,GlobalConfVarNames.COUNTER_AC).getValue()
          + "\tAD_VIEWS\t" + 
          job.getCounters().findCounter(p,GlobalConfVarNames.COUNTER_AV).getValue()
          + "\tUNIQUE_BCOOKIE\t" + 
          job.getCounters().findCounter(p,GlobalConfVarNames.COUNTER_BCOOKIE).getValue()
          + "\n"
        );
      }
      outputStream.writeUTF(sb.toString());
      outputStream.flush();
      outputStream.close();
    }
    return success ? 0 : 1;
  }

  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    int ret = ToolRunner.run(conf, new CookieCounterDriver(), otherArgs);
    System.exit(ret);
  }
}
