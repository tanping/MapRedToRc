package com.yahoo.ccdi.fetl.ao.cookie;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumCombiner extends
    Reducer<Text, IntWritable, Text, IntWritable> {
  static private int sum = 0;
  
  @Override
  public void reduce(Text keyIn, Iterable<IntWritable> valueIn,
      Context context) throws IOException, InterruptedException {
    for (IntWritable val : valueIn) {
      sum += val.get();
    }
    System.out.println("$$$ In combiner : "+ keyIn + " sum = "+ sum);
  }
  
  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
    // aggregate all number of cookies together, emit result once at the end.
    System.out.println("$$$ Combiner emits at end: "+sum);
    context.write(new Text(), new IntWritable(sum));
  }
}
