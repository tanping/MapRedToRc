package com.yahoo.ccdi.fetl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * and counts them. The output is a locally sorted list of words and the 
 * count of how often they occurred.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar wordcount
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 */
public class Rc2Text extends Configured implements Tool {
  
  public static final Log LOG = LogFactory.getLog(Rc2Text.class);
  private static final String delim = "";
  
  /**
   * Counts the words in each line.
   * For each line of input, break the line into words and emit them as
   * (<b>word</b>, <b>1</b>).
   */
  public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, BytesRefArrayWritable, LongWritable, Text> {

@Override
    public void map(LongWritable key, BytesRefArrayWritable value,
        OutputCollector<LongWritable, Text /*Text, IntWritable*/> output, Reporter reporter)
        throws IOException {
       int capacity = value.size();
       int index = 0;
       StringBuilder sb = new StringBuilder();
       // convert value back to string
       Text bcookie = new Text(value.get(index++).getBytesCopy());
       sb.append(bcookie.toString() + delim);
       // convert timestmap (1), 3 tags byte array back to long
       while ( index < 5 ) { 
         ByteArrayInputStream bis = new ByteArrayInputStream(value.get(index++).getBytesCopy()); 
         DataInputStream dis = new DataInputStream(bis); 
         long varLong = dis.readLong(); 
         sb.append(varLong + delim);
       }
       while (index < capacity) {
         byte[] ba = value.get(index++).getBytesCopy();
         Text simple = new Text(ba);
         sb.append(simple.toString() + delim);
       }
       output.collect(key, new Text(sb.toString()));
    }
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
    JobConf conf = new JobConf(getConf(), Rc2Text.class);
    conf.setJobName("Rc2Text");
 
    // the keys are words (strings)
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);
    
    conf.setMapperClass(MapClass.class);        
    conf.setReducerClass(Reduce.class);
    
    ColumnProjectionUtils.setFullyReadColumns(conf);
    conf.setInputFormat(RCFileInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

//    conf.set("mapred.output.compress", "true");
//    conf.set("mapred.output.compression.type","BLOCK");
//    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    
    
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
        return -1;
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        return -1;
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
                         other_args.size() + " instead of 2.");
      return -1;
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
