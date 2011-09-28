package com.yahoo.ccdi.fetl;

import java.io.IOException;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.yahoo.ccdi.fetl.ETLKey;
import com.yahoo.ccdi.fetl.CustomETLValue;
import com.yahoo.ccdi.fetl.FieldSerializer;

public class ABF1PrinterMapper extends MapReduceBase implements
    Mapper<ETLKey, ETLValue, Text, Text> {

  @Override
  public void map(ETLKey key, ETLValue value, OutputCollector output,
      Reporter reporter) throws IOException {
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
