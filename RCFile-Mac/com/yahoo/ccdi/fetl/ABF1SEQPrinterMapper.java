package com.yahoo.ccdi.fetl;

import java.io.IOException;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ABF1SEQPrinterMapper extends MapReduceBase implements
Mapper<ETLKey, ETLValue, ETLKey, LOETLValue> {
  
  @Override
  public void map(ETLKey key, ETLValue value, OutputCollector<ETLKey, LOETLValue> output,
      Reporter reporter) throws IOException {
    
    LOETLValue loetlvalue = new LOETLValue(value);
    output.collect(key, loetlvalue);
  }
}
