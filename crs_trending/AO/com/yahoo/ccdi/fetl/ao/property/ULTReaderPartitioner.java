package com.yahoo.ccdi.fetl.ao.property;

import org.apache.hadoop.mapreduce.Partitioner;

import com.yahoo.ccdi.fetl.GlobalConfVarNames;
import com.yahoo.ccdi.fetl.MetricsKeyType;
import com.yahoo.ccdi.fetl.MetricsValueType;

public class ULTReaderPartitioner extends
    Partitioner<MetricsKeyType, MetricsValueType> {

  @Override
  public int getPartition(MetricsKeyType key, MetricsValueType value,
      int numReduceTasks) {
    int reducer = 0;
    switch (Integer.parseInt((key.getPropertyId()))) {
      case GlobalConfVarNames.ALL_PROPERTY_ID_INT:
        int all_offset = getOffSet(key, GlobalConfVarNames.ALL_REDUCER_OFFSET);
        reducer = all_offset;
        break;
      case GlobalConfVarNames.NEWS_PROPERTY_ID_INT:
       reducer = getReducerNumber(key, 0);
       break;
      case GlobalConfVarNames.FINANCE_PROPERTY_ID_INT:
        reducer = getReducerNumber(key, 1);
        break;
      case GlobalConfVarNames.SPORTS_PROPERTY_ID_INT:
        reducer = getReducerNumber(key, 2);
        break;
      case GlobalConfVarNames.MAIL_PROPERTY_ID_INT:
        reducer = getReducerNumber(key, 3);
        break;
      case GlobalConfVarNames.FRONT_PAGE_PROPERTY_ID_INT:
        reducer = getReducerNumber(key, 4);
        break;
      case GlobalConfVarNames.OMG_PROPERTY_ID_INT:
        reducer = getReducerNumber(key, 5);
        break;
      case GlobalConfVarNames.SEARCH_PROPERTY_ID_INT:
        reducer = getReducerNumber(key, 6);
        break;
      default: 
        System.err.println("Unexcepted property id : "
            + Integer.parseInt((key.getPropertyId())));
        break;
    }
    if (reducer > GlobalConfVarNames.NUM_REDUCERS) {
      System.err.println("ERROR: " +
      		"reducer number returned from partitioner is out of range");
    }
    System.out.println("Reducer number : " + reducer);
    return reducer;
  }

  /**
   * @param key,
            reducerIndex, the index of property reducer
   * @return
   */
  private int getReducerNumber(MetricsKeyType key, int reducerIndex) {
    int property_offset;
    int reducer;
    property_offset = getOffSet(key, GlobalConfVarNames.PROPERTY_REDUCER_OFFSET);
    reducer = GlobalConfVarNames.ALL_REDUCER_OFFSET + 
    reducerIndex * GlobalConfVarNames.PROPERTY_REDUCER_OFFSET + property_offset;
    return reducer;
  }

  /**
   * @param key
   * @return
   */
  private int getOffSet(MetricsKeyType key, int offset) {
    return (key.getBcookie().hashCode() & Integer.MAX_VALUE) % offset;
  }
}
