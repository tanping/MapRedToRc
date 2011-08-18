package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
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
public class WordCountRc2Text extends Configured implements Tool {
  
  
  /**
   * Counts the words in each line.
   * For each line of input, break the line into words and emit them as
   * (<b>word</b>, <b>1</b>).
   */
  public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, /*Text*/BytesRefArrayWritable, 
 Text, /*Text*/IntWritable> {

    public static final Log LOG = LogFactory
        .getLog("org.apache.hadoop.examples.WordCountRc2Text");

    // private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();

    private static ColumnarSerDe serDe;

    private static Configuration hconf = new Configuration();

    private static Properties tbl;

    static {
      try {
        // the SerDe part is from TestLazySimpleSerDe
        serDe = new ColumnarSerDe();
        // Create the SerDe
        tbl = createProperties();
        serDe.initialize(hconf, tbl);
      } catch (Exception e) {
      }
    }

    private static Properties createProperties() {
      Properties tbl = new Properties();

      // Set the configuration parameters
      tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
      tbl.setProperty("columns", "astring,aint");
      tbl.setProperty("columns.types", "string:");
      tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
      return tbl;
    }

    // @Override
    public void map(LongWritable key, BytesRefArrayWritable value,
        OutputCollector<Text, /*Text*/IntWritable> output, Reporter reporter)
        throws IOException {
       // convert value back to string
       Text keyT = new Text(value.get(0).getBytesCopy());
       //Text valueT = new Text(value.get(1).getBytesCopy());
       int v = byteArrayToInt(value.get(1).getBytesCopy());
       IntWritable valueI = new IntWritable(v);
       output.collect(keyT, valueI);

////  Using Serde to deserialize     
//      value.resetValid(2);
//      Object row;
//      Text keyT = new Text();
//      Text valueT = new Text();
//      IntWritable valueI = new IntWritable();
//
//      try {
//        row = serDe.deserialize(value);
//
//        StructObjectInspector oi;
//
//        oi = (StructObjectInspector) serDe.getObjectInspector();
//
//        List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
//
//        System.out.println("### Field size should 2. Actually it is "
//            + fieldRefs.size());
//
//        for (int i = 0; i < fieldRefs.size(); i++) {
//          Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
//          Object standardWritableData = ObjectInspectorUtils
//              .copyToStandardObject(fieldData, fieldRefs.get(i)
//                  .getFieldObjectInspector(),
//                  ObjectInspectorCopyOption.WRITABLE);
//
//          System.out.println("### standardWritableData["
//              + standardWritableData+ "]");
//          
//          if (i == 0) {
//            keyT = (Text) standardWritableData;
//          } else if (i == 1) {
//            //valueT = (Text) standardWritableData;
//            valueI = (IntWritable)standardWritableData;
//          }
//
//        }
//      } catch (SerDeException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      }
      //////
      output.collect(keyT, valueI);
    }
  }
  
  /**
   * Convert the byte array to an int.
   *
   * @param b The byte array
   * @return The integer
   */
  public static int byteArrayToInt(byte[] b) {
      return byteArrayToInt(b, 0);
  }
  
  /**
   * Convert the byte array to an int starting from the given offset.
   *
   * @param b The byte array
   * @param offset The array offset
   * @return The integer
   */
  public static int byteArrayToInt(byte[] b, int offset) {
      int value = 0;
      for (int i = 0; i < 4; i++) {
          int shift = (4 - 1 - i) * 8;
          value += (b[i + offset] & 0x000000FF) << shift;
      }
      return value;
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
    JobConf conf = new JobConf(getConf(), WordCountRc2Text.class);
    conf.setJobName("wordcount_Rc2Text");
 
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
   // conf.setOutputValueClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setMapperClass(MapClass.class);        
//    conf.setCombinerClass(Reduce.class);
//    conf.setReducerClass(Reduce.class);
    
    ColumnProjectionUtils.setFullyReadColumns(conf);
    conf.setInputFormat(RCFileInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
//    ArrayList<Integer> readCols = new ArrayList<Integer>();
//    readCols.add(Integer.valueOf(1));
//    readCols.add(Integer.valueOf(2));
//    ColumnProjectionUtils.setReadColumnIDs(conf, readCols);

    
    
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
    int res = ToolRunner.run(new Configuration(), new WordCountRc2Text(), args);
    System.exit(res);
  }

}
