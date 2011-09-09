package com.yahoo.ccdi.fetl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yahoo.ccdi.fetl.Rc2Text.MapClass;
import com.yahoo.ccdi.fetl.Rc2Text.Reduce;


public class SlimLOSeq2Text extends Configured implements Tool{

  
  public static final Log LOG = LogFactory.getLog(SlimLOSeq2Text.class);
  private static final String delim = "";
  
  /**
   * Counts the words in each line.
   * For each line of input, break the line into words and emit them as
   * (<b>word</b>, <b>1</b>).
   */
  public static class MapClass extends MapReduceBase
    implements Mapper<ETLKey, LOETLValue, Text, Text> {

@Override
    public void map(ETLKey key, LOETLValue value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
  
  String bcookie = key.getBcookie().toString("UTF-8");

  String keyStr = "Bcookie = " + bcookie + "TimeStamp = "
      + key.getTimestamp();
  
  String valStr = new String();

  valStr += ""+value.getDhrTag();
  valStr += ""+value.getFilterTag();
  valStr += ""+value.getTransformErrorTag();
  valStr += ""+FieldSerializer.mapToString(value.getSimpleFields());
  valStr += ""+FieldSerializer.mapOfMapToString(value.getMapFields());
  valStr += ""+FieldSerializer.listMapToString(value.getMapListFields());
       output.collect(new Text(keyStr), new Text(valStr));
    }
  }
  
  static int printUsage() {
    System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
  public static class Reduce extends MapReduceBase
  implements Reducer<LongWritable, Text, LongWritable, Text> {
  
  public void reduce(LongWritable key, Iterator<Text> values,
                     OutputCollector<LongWritable, Text> output, 
                     Reporter reporter) throws IOException {
    while (values.hasNext()) {
    output.collect(key, values.next());
    }
  }
}
  
  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), SlimLOSeq2Text.class);
    conf.setJobName("SlimLOSeq2Text");
 
    // the keys are words (strings)
    conf.setOutputKeyClass(LongWritable.class);
    // the values are counts (ints)
    conf.setOutputValueClass(Text.class);
    
    conf.setMapperClass(MapClass.class);        
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    ColumnProjectionUtils.setFullyReadColumns(conf);
    conf.setInputFormat(RCFileInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
//    // set RC file reader only reads columns 2, 3
//    ArrayList<Integer> readCols = new ArrayList<Integer>();
//    readCols.add(Integer.valueOf(1));
//    readCols.add(Integer.valueOf(2));
//    ColumnProjectionUtils.setReadColumnIDs(conf, readCols);

    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.type","BLOCK");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    
    
    conf.set("mapred.job.queue.name", conf.get("mapred.job.queue.name"));
    conf.set("mapred.job.queue.name", "audience_fetl");
    
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
    int res = ToolRunner.run(new Configuration(), new Rc2Text(), args);
    System.exit(res);
  }


}
