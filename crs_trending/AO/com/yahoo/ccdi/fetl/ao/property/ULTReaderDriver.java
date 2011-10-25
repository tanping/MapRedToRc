package com.yahoo.ccdi.fetl.ao.property;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yahoo.ccdi.fetl.GlobalConfVarNames;
import com.yahoo.ccdi.fetl.MetricsKeyType;
import com.yahoo.ccdi.fetl.MetricsTypeTextOutputFormat;
import com.yahoo.ccdi.fetl.MetricsValueType;
//import com.yahoo.ccdi.fetl.SequenceProjectorFormatExt;
import com.yahoo.ccdi.fetl.sequence.mapreduce.SequenceProjectorFormat;

public class ULTReaderDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String queueName = conf.get("mapred.job.queue.name");
    if (queueName == null || queueName.isEmpty()) {
      conf.set("mapred.job.queue.name", "audience_fetl");
    } else {
      conf.set("mapred.job.queue.name", queueName);
    }
    

    String alloffset = conf.get("alloffset");
    if (alloffset == null || alloffset.isEmpty()) {
      conf.set("alloffset", "512");
    }
    String propoffset = conf.get("propoffset");
    if (propoffset == null || propoffset.isEmpty()) {
      conf.set("propoffset", "8");
    }
    
    int numAllOffset = 0;
    try {
      numAllOffset = Integer.parseInt(alloffset);
    } catch (Exception e){
      numAllOffset = GlobalConfVarNames.ALL_REDUCER_OFFSET;
      e.printStackTrace();
    }
    
    int numPropOffset = 0;
    try {
    numPropOffset = Integer.parseInt(propoffset);
    } catch (Exception e){
      numPropOffset = GlobalConfVarNames.PROPERTY_REDUCER_OFFSET;
      e.printStackTrace();
    }
    
    int numReduceTasks = 
      numAllOffset + GlobalConfVarNames.NUM_PROPERTY * numPropOffset;
    
    String isReg = conf.get("register");
    if (isReg == null || isReg.isEmpty()) {
      conf.set("register", "true");
    }
    
    Job job = new Job(conf);
    job.setJarByClass(ULTReaderDriver.class);
    job.setJobName(conf.get("mapred.job.name", "crs_trending_property_metrics"));

    String projFields = conf.get("projectedfields");
    if ( projFields == null || projFields.equals("") ) {
      System.err.println("Must provide fields to be projected.");
      return -1;
    }
    
    SequenceProjectorFormat
        .setProjections(
            job,
            "",
            projFields);
    
    
    job.setInputFormatClass(SequenceProjectorFormat.class);
    job.setOutputFormatClass(MetricsTypeTextOutputFormat.class);
    job.setMapperClass(ULTReaderMapper.class);
    job.setPartitionerClass(ULTReaderPartitioner.class);
    job.setReducerClass(ULTReaderReducer.class);

    job.setMapOutputKeyClass(MetricsKeyType.class);
    job.setMapOutputValueClass(MetricsValueType.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

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
    
    //int numReduceTasks = GlobalConfVarNames.NUM_REDUCERS;
    job.setNumReduceTasks(numReduceTasks);

    boolean success = job.waitForCompletion(true);
    if (job.isSuccessful()) {
      FileSystem hdfs = FileSystem.get(conf);
      Path path = new Path(output + GlobalConfVarNames.METRICS_SUMMARY);
      if (hdfs.exists(path)) {
        hdfs.delete(path, false);
      }
      FSDataOutputStream outputStream = hdfs.create(path);
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
      String propName = "";
      for (String property : propArray) {
        propName = propertyIDtoName(property);
        sb.append( propName
          +"\tPAGE_VIEWS\t" + 
          job.getCounters().findCounter(property,GlobalConfVarNames.COUNTER_PV).getValue()
          + "\tAD_CLICKS\t" + 
          job.getCounters().findCounter(property,GlobalConfVarNames.COUNTER_AC).getValue()
          + "\tAD_VIEWS\t" + 
          job.getCounters().findCounter(property,GlobalConfVarNames.COUNTER_AV).getValue()
          + "\tUNIQUE_BCOOKIE\t" + 
          job.getCounters().findCounter(property,GlobalConfVarNames.COUNTER_BCOOKIE).getValue()
          + "\n"
        );
      }
      outputStream.writeUTF(sb.toString());
      outputStream.flush();
      outputStream.close();
    }
    return success ? 0 : 1;
  }
  
  private String propertyIDtoName(String propID){
    if (propID.equals(GlobalConfVarNames.ALL_PROPERTY_ID)) {
      return GlobalConfVarNames.ALL_PROPERTY;
    } else if (propID.equals(GlobalConfVarNames.MAIL_PROPERTY_ID)) {
      return GlobalConfVarNames.MAIL_PROPERTY;
    } else if (propID.equals(GlobalConfVarNames.FRONT_PAGE_PROPERTY_ID)) {
      return GlobalConfVarNames.FRONT_PAGE_PROPERTY;
    } else if (propID.equals(GlobalConfVarNames.NEWS_PROPERTY_ID)) {
      return GlobalConfVarNames.NEWS_PROPERTY;
    } else if (propID.equals(GlobalConfVarNames.FINANCE_PROPERTY_ID)) {
      return GlobalConfVarNames.FINANCE_PROPERTY;
    } else if (propID.equals(GlobalConfVarNames.SEARCH_PROPERTY_ID)) {
      return GlobalConfVarNames.SEARCH_PROPERTY;
    } else if (propID.equals(GlobalConfVarNames.SPORTS_PROPERTY_ID)) {
      return GlobalConfVarNames.SPORTS_PROPERTY;
    } else if (propID.equals(GlobalConfVarNames.OMG_PROPERTY_ID)) {
      return GlobalConfVarNames.OMG_PROPERTY;
    } 
    return GlobalConfVarNames.UNKNOWN_PROPERTY;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    int ret = ToolRunner.run(conf, new ULTReaderDriver(), otherArgs);
    System.exit(ret);
  }
}
