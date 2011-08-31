package com.yahoo.ccdi.fetl;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import com.yahoo.ccdi.fetl.ETLRCKeyValue;
import com.yahoo.ccdi.fetl.ETLKey;
import com.yahoo.ccdi.fetl.ETLValue;
import com.yahoo.ccdi.fetl.FieldSerializer;
import org.apache.hadoop.record.Buffer;

public class ABF1RCPrinterMapper extends MapReduceBase implements
    Mapper<ETLKey, ETLValue, LongWritable, LOKeyValue> {

  @Override
  public void map(ETLKey key, ETLValue value, OutputCollector output,
      Reporter reporter) throws IOException {
//    Buffer bcookie = new Buffer();
//    bcookie = key.getBcookie();
//    long ts = (long) 0;
//    ts = key.getTimestamp();
//
//    String keyStr = "Bcookie = " + bcookie + " :: TimeStamp = " + ts;
//    if (bcookie == null) {
//      bcookie = new Buffer();
//      System.out.println("BCookie is null.");
//    }

//    String valStr = new String();
//    valStr += FieldSerializer.mapToString(value.getSimpleFields());
//    valStr += FieldSerializer.mapOfMapToString(value.getMapFields());
//    valStr += FieldSerializer.listMapToString(value.getMapListFields());

    //ETLRCKeyValue rcKeyValue = new ETLRCKeyValue(key, value);
    LOKeyValue keyValue = new LOKeyValue(key, value);
    output.collect(new LongWritable(1), keyValue);
  }
}

