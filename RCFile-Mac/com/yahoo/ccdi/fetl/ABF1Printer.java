package com.yahoo.ccdi.fetl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class ABF1Printer {

    public static void main(String[] args) throws Exception {        
        Configuration conf = new Configuration();
        
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
    
        JobConf ABF1Print = new JobConf(ABF1Printer.class);
        
        ABF1Print.setJobName(conf.get("mapred.job.name", "abf1_print"));
        ABF1Print.setMapperClass(ABF1PrinterMapper.class);
        ABF1Print.setReducerClass(IdentityReducer.class);
        
        ABF1Print.setInputFormat(SequenceFileInputFormat.class);
        ABF1Print.setOutputFormat(TextOutputFormat.class);
        
        ABF1Print.setMapOutputKeyClass(Text.class);
        ABF1Print.setMapOutputValueClass(Text.class);
        
        ABF1Print.setOutputKeyClass(Text.class);
        ABF1Print.setOutputValueClass(Text.class);
        
        ABF1Print.setCompressMapOutput(true);
        
        ABF1Print.set("mapred.job.queue.name", conf.get("mapred.job.queue.name"));
        ABF1Print.set("mapred.job.queue.name", "audience_fetl");

        // compress
    ABF1Print.set("mapred.output.compress", "true");
    ABF1Print.set("mapred.output.compression.type", "BLOCK");
    ABF1Print.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        
        FileInputFormat.setInputPaths(ABF1Print, otherArgs[0]);
        FileOutputFormat.setOutputPath(ABF1Print, new Path(otherArgs[1]));

        if(conf.get("projection.fields") != null) {
            ABF1Print.set("projection.fields",conf.get("projection.fields"));    
        }
        
        if(otherArgs.length != 3) {
            ABF1Print.setNumReduceTasks(0);
        } else {
            System.out.println(otherArgs[2]);
            ABF1Print.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
        }
  
        RunningJob myPrinter = JobClient.runJob(ABF1Print);
    }
}
