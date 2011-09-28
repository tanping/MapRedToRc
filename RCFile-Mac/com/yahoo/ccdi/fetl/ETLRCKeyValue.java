package com.yahoo.ccdi.fetl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;

public class ETLRCKeyValue extends BytesRefArrayWritable implements Cloneable{
  
  public ETLRCKeyValue() {}

  public ETLRCKeyValue(ETLKey eKey, CustomETLValue eValue) {
    
    // flatten key data structure
    Buffer bcookie = eKey.getBcookie();
    if (bcookie == null) bcookie = new Buffer();
    long timestamp = 0;
    timestamp = eKey.getTimestamp();
//    System.out.println("### keyString is "+keyStr);
    
    int index = 0;
    // convert to byte[]
    byte[] bcBytes = bcookie.get();
    byte[] tsBytes = WritableUtils.toByteArray(new LongWritable(timestamp));
    // set bytesRefWritables
    set(index++, new BytesRefWritable(bcBytes));
    set(index++, new BytesRefWritable(tsBytes));
    
    // flatten value data structure
    String valStr = new String();
    try {
      valStr += FieldSerializer.mapToString(eValue.getSimpleFields());
      Iterator itSimple = eValue.getSimpleFields().entrySet().iterator();
      Buffer b = new Buffer();
      while (itSimple.hasNext()) {
          Map.Entry<String, Buffer> entry = (Map.Entry<String, Buffer>)itSimple.next();
//          byte[] keyBytes = entry.getKey().getBytes();
//          byte[] bufferBytes = entry.getValue().get();
          b.append(entry.getKey().getBytes());
          b.append(entry.getValue().get());
          // setbytesRefWritables
//          // key -- String
//          set(index++, new BytesRefWritable(keyBytes));
//          // value -- Buffer
//          set(index++, new BytesRefWritable(bufferBytes));
      }
      set(index++, new BytesRefWritable(b.get()));
      ////////////// Map of map
      b = new Buffer();
      TreeMap<String,TreeMap<String,Buffer>> mapFields = eValue.getMapFields();
      Iterator itMapMap = mapFields.entrySet().iterator();
      while (itMapMap.hasNext()) {
          Map.Entry<String, TreeMap<String, Buffer>> mapMapEntry = 
            (Map.Entry<String, TreeMap<String, Buffer>>)itMapMap.next();
          // key of map
          b.append(mapMapEntry.getKey().getBytes());
          // setbytesRefWritables
          //set(index++, new BytesRefWritable(mapOfMapKeyBytes));
          // value of map
          b.append(mapToBuffer(mapMapEntry.getValue()).get());
          //set(index++, new BytesRefWritable(value.getBytes()));
      }
      set(index++, new BytesRefWritable(b.get()));
      ///////////////// list of Map
      b = new Buffer();
      TreeMap<String,ArrayList<TreeMap<String,Buffer>>> listMapFields = 
        eValue.getMapListFields();
      Iterator itListMap = listMapFields.entrySet().iterator();
      while (itListMap.hasNext()) {
          Map.Entry<String, ArrayList<TreeMap<String, Buffer>>> listMapEntry = 
            (Map.Entry<String, ArrayList<TreeMap<String, Buffer>>>)itListMap.next();
          //output += listMapEntry.getKey() + " = " + serializeListMap(listMapEntry.getValue());
          b.append( listMapEntry.getKey().getBytes());
          b.append(serializeListMap(listMapEntry.getValue()).get());
      }
      set(index++, new BytesRefWritable(b.get()));
      
    } catch ( IOException e ) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    valid = index;
    System.out.println(valid);
  }
  
  public static Buffer mapToBuffer(TreeMap<String, Buffer> map) throws IOException {
    Buffer output = new Buffer(); 
    Iterator itr = map.entrySet().iterator();
    //boolean first = true;
    while (itr.hasNext()) {
//        if (first) {
//            first = false;
//        } else {
//            output.append(" ".getBytes());
//        }
        Map.Entry<String, Buffer> entry = (Map.Entry<String, Buffer>)itr.next();
        output.append(entry.getKey().getBytes());
        //output.append(" ".getBytes());
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
