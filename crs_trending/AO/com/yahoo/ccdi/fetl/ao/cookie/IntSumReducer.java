package com.yahoo.ccdi.fetl.ao.cookie;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends
    Reducer<Text, IntWritable, Text, IntWritable> {
  static private int sum = 0;
  
  @Override
  public void reduce(Text keyIn, Iterable<IntWritable> valueIn,
      Context context) throws IOException, InterruptedException {
    for (IntWritable val : valueIn) {
      sum += val.get();
    }
    System.out.println("&&& Reducer sum = "+sum);
  }
  
  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
    System.out.println("&&&  Reducer emits at end: " + sum);
  }
}
