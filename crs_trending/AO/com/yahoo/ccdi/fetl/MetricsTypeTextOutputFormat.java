package com.yahoo.ccdi.fetl;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

import com.yahoo.ccdi.fetl.fetl_hadoop_io.mapreduce.lib.output.MultipleOutputFormat;

public class MetricsTypeTextOutputFormat extends MultipleOutputFormat<Text, Text> {

  @Override
  protected ArrayList<String> generateFileNameForKeyValue(Text keyIn, Text value) {

    ArrayList<String> result = new ArrayList<String>();

    String propertyId = keyIn.toString();
    
    if (propertyId.equals(GlobalConfVarNames.MAIL_PROPERTY_ID)) {
      result.add(GlobalConfVarNames.MAIL_DIR);
    } else if (propertyId.equals(GlobalConfVarNames.FRONT_PAGE_PROPERTY_ID)) {
      result.add(GlobalConfVarNames.FRONT_PAGE_DIR);
    } else if (propertyId.equals(GlobalConfVarNames.NEWS_PROPERTY_ID)) {
      result.add(GlobalConfVarNames.NEWS_DIR);
    } else if (propertyId.equals(GlobalConfVarNames.FINANCE_PROPERTY_ID)) {
      result.add(GlobalConfVarNames.FINANCE_DIR);
    } else if (propertyId.equals(GlobalConfVarNames.SEARCH_PROPERTY_ID)) {
      result.add(GlobalConfVarNames.SEARCH_DIR);
    } else if (propertyId.equals(GlobalConfVarNames.SPORTS_PROPERTY_ID)) {
      result.add(GlobalConfVarNames.SPORTS_DIR);
    } else if (propertyId.equals(GlobalConfVarNames.OMG_PROPERTY_ID)) {
      result.add(GlobalConfVarNames.OMG_DIR);
    } else if (propertyId.equals(GlobalConfVarNames.ALL_PROPERTY_ID)) {
      result.add(GlobalConfVarNames.ALL_DIR);
    }
    return result;
  }
  
  @Override
  protected ArrayList<String> getFileNames(){
    ArrayList<String> result =  new ArrayList<String>();
    result.add(GlobalConfVarNames.MAIL_DIR);
    result.add(GlobalConfVarNames.FRONT_PAGE_DIR);
    result.add(GlobalConfVarNames.NEWS_DIR);
    result.add(GlobalConfVarNames.FINANCE_DIR);
    result.add(GlobalConfVarNames.SEARCH_DIR);
    result.add(GlobalConfVarNames.SPORTS_DIR);
    result.add(GlobalConfVarNames.OMG_DIR);
    result.add(GlobalConfVarNames.ALL_DIR);
    return result;
  }
}
