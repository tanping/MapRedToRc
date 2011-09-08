package com.yahoo.ccdi.fetl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.record.RecordOutput;

public class LOKeyValue extends BytesRefArrayWritable implements Cloneable{
  
  private static final String mapDelim = "";
  private static final String listDelim = "";
  private long filterTag;
  private long dhrTag;
  private long transformErrorTag;
  private TreeMap<String, Buffer> simpleFields;
  private TreeMap<String, TreeMap<String, Buffer>> mapFields;
  private TreeMap<String, ArrayList<TreeMap<String, Buffer>>> mapListFields;

  public LOKeyValue() {
  }

  public LOKeyValue(ETLKey eKey, ETLValue eValue) {
    
    // flatten key data structure
    Buffer bcookie = eKey.getBcookie();
    long timestamp = 0;
    timestamp = eKey.getTimestamp();

    int index = 0;
    // convert to byte[]
    byte[] bcBytes = bcookie.get();
    byte[] tsBytes = WritableUtils.toByteArray(new LongWritable(timestamp));
    // set bytesRefWritables
    set(index++, new BytesRefWritable(bcBytes)); //1
    set(index++, new BytesRefWritable(tsBytes)); //2
    
    // tags
    byte[] dhrTagBytes = WritableUtils.toByteArray(new LongWritable(
        eValue.getDhrTag()));
    set(index++, new BytesRefWritable(dhrTagBytes));  //3
    byte[] filterTagBytes = WritableUtils.toByteArray(new LongWritable(
        eValue.getFilterTag()));
    set(index++, new BytesRefWritable(filterTagBytes)); //4
    byte[] transformErrorTag = WritableUtils.toByteArray(new LongWritable(
        eValue.getTransformErrorTag()));
    set(index++, new BytesRefWritable(transformErrorTag));  //5
    
    // 6 + (2+ 26 + 4 + 1) -1 = 38
    //  simplefields => TreeMap
    TreeMap<String, Buffer> simpleFields = eValue.getSimpleFields();
    // feed
    Buffer entry = simpleFields.get("feed");
    if (entry == null) {
      set(index++, new BytesRefWritable());
    } else {
      set(index++, new BytesRefWritable(entry.get()));
    }
    // datestamp
    entry = simpleFields.get("datestamp");
    if (entry == null) {
      set(index++, new BytesRefWritable());
    } else {
      set(index++, new BytesRefWritable(entry.get()));
    }
    // going over every utlSimpleFields key 
    // 26 total
    for (int i = 0; i < UltRequiredFiledName.ultSimpleFields.length; i++) {
      String requiredKey = UltRequiredFiledName.ultSimpleFields[i];
      entry = simpleFields.get(requiredKey); 
      if (entry == null) {
        set(index++, new BytesRefWritable());
      } else {
        set(index++, new BytesRefWritable(entry.get()));
      }
    }
    
      // mapFields => map of map 
      // 4 total
      TreeMap<String,TreeMap<String,Buffer>> mapFields = eValue.getMapFields();
      // go over requiredKeys for mapFields
      for (int i = 0; i < UltRequiredFiledName.ultMapFields.length; i++) {
        String requiredKey = UltRequiredFiledName.ultMapFields[i];
        TreeMap<String, Buffer> requiredValue = 
          (TreeMap<String, Buffer> )mapFields.get(requiredKey);
        Buffer b = new Buffer();
        if (requiredValue != null) {
          try {
            b = new Buffer(mapToBuffer(requiredValue).get());
          } catch ( IOException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        set(index++, new BytesRefWritable(b.get()));
      }
      
//      Buffer b = new Buffer();
//      while (itMapMap.hasNext()) {
//          Map.Entry<String, TreeMap<String, Buffer>> mapMapEntry = 
//            (Map.Entry<String, TreeMap<String, Buffer>>)itMapMap.next();
//          // key of map
//          b.append(mapMapEntry.getKey().getBytes());
//          // setbytesRefWritables
//          //set(index++, new BytesRefWritable(mapOfMapKeyBytes));
//          // value of map
//          b.append(mapToBuffer(mapMapEntry.getValue()).get());
//          //set(index++, new BytesRefWritable(value.getBytes()));
//      }
//      set(index++, new BytesRefWritable(b.get()));
      
      ///////////////// list of Map 
      // 1 total
      TreeMap<String, ArrayList<TreeMap<String, Buffer>>> mapListFieds = eValue.getMapListFields();
      // go over requiredKeys for mapFields
      for (int i = 0; i < UltRequiredFiledName.ultMapListFields.length; i++) {
        String requiredKey = UltRequiredFiledName.ultMapListFields[i];
        ArrayList<TreeMap<String, Buffer>> requiredValue = mapListFieds.get(requiredKey);
        Buffer b = new Buffer();
        if (requiredValue != null) {
          try {
            b = new Buffer(serializeListMap(requiredValue).get());
          } catch ( IOException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        set(index++, new BytesRefWritable(b.get()));
      }     
    } 
  
  public static Buffer mapToBuffer(TreeMap<String, Buffer> map) throws IOException {
    Buffer output = new Buffer(); 
    Iterator itr = map.entrySet().iterator();
    boolean first = true;
    while (itr.hasNext()) {
        if (first) {
            first = false;
        } else {
            output.append(mapDelim.getBytes());
        }
        Map.Entry<String, Buffer> entry = (Map.Entry<String, Buffer>)itr.next();
        output.append(entry.getKey().getBytes());
        output.append(mapDelim.getBytes());
        output.append(entry.getValue().get());
    }

    return output;
}
  
  public static Buffer serializeListMap(ArrayList<TreeMap<String, Buffer>> listMap) throws IOException {
    Buffer output = new Buffer();    //"[";
    for (int i = 0; i < listMap.size(); i++) {
        //if (i != 0) output += " ";
        output.append( mapToBuffer(listMap.get(i)).get());
    }
    //output += ']';
    return output;
}

}
