package com.yahoo.ccdi.fetl.ao.property;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.record.Buffer;

import com.yahoo.ccdi.fetl.GlobalConfVarNames;
import com.yahoo.ccdi.fetl.MetricsKeyType;
import com.yahoo.ccdi.fetl.MetricsValueType;

public class ULTReaderReducer extends
    Reducer<MetricsKeyType, MetricsValueType, Text, Text> {

  static private String lastBcookie = "";
  static private long uniqueBcookieCount = 0;
  static private MetricsValueType valueOut = new MetricsValueType();
  static private String reducerPropertyId = "";
  
  
  @Override
  public void reduce(MetricsKeyType keyIn, Iterable<MetricsValueType> valueIn,
      Context context) throws IOException, InterruptedException {
    
    String propertyId = keyIn.getPropertyId();
    if (reducerPropertyId.equals("")) {
      reducerPropertyId = propertyId;
    }
    if (!reducerPropertyId.equals(propertyId)) {
      throw new IOException(
          "Multiple property IDs showed up in a single reducer.  Property ID " +
          reducerPropertyId + " and " + propertyId
       );
    }
    String bcookie = keyIn.getBcookie();

    if ((!bcookie.equals("")) && (bcookie != null) 
        && (!bcookie.equals(lastBcookie))) {
      lastBcookie = bcookie;
      uniqueBcookieCount++;
    }
    for (MetricsValueType value : valueIn) {
      valueOut.add(value);
    }
  }
  
  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
    String textOut = "\tPAGE_VIEWS\t" + valueOut.getPv()
    + "\tAD_CLICKS\t" + valueOut.getAdClicks()
    + "\tAD_VIEWS\t" + valueOut.getAdViews()
    + "\tUNIQUE_BCOOKIE\t" + uniqueBcookieCount;

    context.write(new Text(reducerPropertyId), new Text(textOut));
    
    // put into context counter
    // PV
    context.getCounter(reducerPropertyId,
        GlobalConfVarNames.COUNTER_PV).increment(valueOut.getPv());
    // AC
    context.getCounter(reducerPropertyId,
        GlobalConfVarNames.COUNTER_AC).increment(valueOut.getAdClicks());
    // AV
    context.getCounter(reducerPropertyId,
        GlobalConfVarNames.COUNTER_AV).increment(valueOut.getAdViews());
    // Cookie
    context.getCounter(reducerPropertyId,
        GlobalConfVarNames.COUNTER_BCOOKIE).increment(uniqueBcookieCount);
    
  }
}
