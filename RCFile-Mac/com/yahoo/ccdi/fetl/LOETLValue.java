package com.yahoo.ccdi.fetl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.record.RecordInput;
import org.apache.hadoop.record.RecordOutput;


public class LOETLValue extends Record{

  private long filterTag;
  private long dhrTag;
  private long transformErrorTag;
  private Buffer feed;
  private Buffer datestamp;
  private TreeMap<String, Buffer> simpleFields;
  private TreeMap<String, TreeMap<String, Buffer>> mapFields;
  private TreeMap<String, ArrayList<java.util.TreeMap<String, Buffer>>> mapListFields;
  
  public LOETLValue() {}
  
  public LOETLValue(ETLValue eValue) {
    this.filterTag = eValue.getFilterTag();
    this.dhrTag = eValue.getDhrTag();
    this.transformErrorTag = eValue.getTransformErrorTag();
    this.simpleFields = eValue.getSimpleFields();
    this.mapFields = eValue.getMapFields();
    this.mapListFields = eValue.getMapListFields();
  }

  public long getFilterTag() {
    return filterTag;
  }
  public void setFilterTag(final long filterTag) {
    this.filterTag=filterTag;
  }
  public long getDhrTag() {
    return dhrTag;
  }
  public void setDhrTag(final long dhrTag) {
    this.dhrTag=dhrTag;
  }
  public long getTransformErrorTag() {
    return transformErrorTag;
  }
  public void setTransformErrorTag(final long transformErrorTag) {
    this.transformErrorTag=transformErrorTag;
  }
  public Buffer getFeed() {
    return this.feed;
  }
  public void setFeed(final Buffer feed) {
    this.feed = feed;
  }
  public Buffer getDatestamp() {
    return this.datestamp;
  }
  public void setDatestamp(final Buffer feed) {
    this.datestamp = datestamp;
  }
  public java.util.TreeMap<String,org.apache.hadoop.record.Buffer> getSimpleFields() {
    return simpleFields;
  }
  public void setSimpleFields(final java.util.TreeMap<String,org.apache.hadoop.record.Buffer> simpleFields) {
    this.simpleFields=simpleFields;
  }
  public java.util.TreeMap<String,java.util.TreeMap<String,org.apache.hadoop.record.Buffer>> getMapFields() {
    return mapFields;
  }
  public void setMapFields(final java.util.TreeMap<String,java.util.TreeMap<String,org.apache.hadoop.record.Buffer>> mapFields) {
    this.mapFields=mapFields;
  }
  public java.util.TreeMap<String,java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>> getMapListFields() {
    return mapListFields;
  }
  public void setMapListFields(final java.util.TreeMap<String,java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>> mapListFields) {
    this.mapListFields=mapListFields;
  }
  
  @Override
  public void serialize(RecordOutput rout, String tag) throws IOException {
    rout.startRecord(this, tag);
    // three tags
    rout.writeLong(filterTag, "filterTag");
    rout.writeLong(dhrTag, "dhrTag");
    rout.writeLong(transformErrorTag, "transformErrorTag");

    // required simple fields, list of Buffer
    {
      // feed
      Buffer entry = simpleFields.get("feed");
      if (entry == null) {
        rout.writeBuffer(new Buffer(), "feedTag");
      }else {
        rout.writeBuffer(entry, "feedTag");
      }
      // datestamp
      entry = simpleFields.get("datestamp");
      if (entry != null) {
        rout.writeBuffer(entry, "datestampTag");
      }
      // required simplefields
      for (int i = 0; i < UltRequiredFiledName.ultSimpleFields.length; i++) {
        String requiredKey = UltRequiredFiledName.ultSimpleFields[i];
        entry = simpleFields.get(requiredKey);
        if (entry == null) {
          rout.writeBuffer(new Buffer(), "simpleFieldValueTag");
        } else {
          rout.writeBuffer(entry, "simpleFieldValueTag");
        }
      }
    }
    // map fields
    {
      for (int i = 0; i < UltRequiredFiledName.ultMapFields.length; i++) {
        String requiredKey = UltRequiredFiledName.ultMapFields[i];
        TreeMap<String, Buffer> requiredValue = 
          (TreeMap<String, Buffer> )mapFields.get(requiredKey);
        Buffer b = new Buffer();
        if (requiredValue == null) {
          rout.writeBuffer(new Buffer(), "mapFieldValueTag");
        } else {
          // requiredValue is a map
          rout.startMap(requiredValue, "mapFieldMapValueTag");
          Set<Entry<String, Buffer>> entry = requiredValue.entrySet();
          for (Iterator<Entry<String, Buffer>> it = entry.iterator(); it.hasNext();) {
            Entry<String, Buffer> insideEntry = it.next();
            rout.writeString(insideEntry.getKey(), "insideKey");
            rout.writeBuffer(insideEntry.getValue(), "insideValue"); 
          }
          rout.endMap(requiredValue,"mapFieldMapValueTag");
        }
      }
    }
    // list of map fields
    {
      for (int i = 0; i < UltRequiredFiledName.ultMapListFields.length; i++) {
        String requiredKey = UltRequiredFiledName.ultMapListFields[i];
        ArrayList<TreeMap<String, Buffer>> requiredValue = mapListFields.get(requiredKey);
        Buffer b = new Buffer();
        if ( requiredValue == null) {
          rout.writeBuffer(new Buffer(), "listMapFiledValueTag");
        } else {
          // requiredValue is a a list of map
          rout.startVector(requiredValue,"listMapFiledValueTag");
          int _rio_len1 = requiredValue.size();
          for(int _rio_vidx1 = 0; _rio_vidx1<_rio_len1; _rio_vidx1++) {
            java.util.TreeMap<String,org.apache.hadoop.record.Buffer> _rio_e1 = requiredValue.get(_rio_vidx1);
            {
              rout.startMap(_rio_e1,"_rio_e1");
              java.util.Set<java.util.Map.Entry<String,org.apache.hadoop.record.Buffer>> _rio_es2 = _rio_e1.entrySet();
              for(java.util.Iterator<java.util.Map.Entry<String,org.apache.hadoop.record.Buffer>> _rio_midx2 = _rio_es2.iterator(); _rio_midx2.hasNext();) {
                java.util.Map.Entry<String,org.apache.hadoop.record.Buffer> _rio_me2 = _rio_midx2.next();
                String _rio_k2 = _rio_me2.getKey();
                org.apache.hadoop.record.Buffer _rio_v2 = _rio_me2.getValue();
                rout.writeString(_rio_k2,"_rio_k2");
                rout.writeBuffer(_rio_v2,"_rio_v2");
              }
              rout.endMap(_rio_e1,"_rio_e1");
            }
          }
          rout.endVector(requiredValue,"listMapFiledValueTag");
        }
      }
    }
    rout.endRecord(this, tag);
  }

  @Override
  public void deserialize(RecordInput rin, String tag) throws IOException {
    rin.startRecord(tag);
    
    rin.endRecord(tag);
  }

  @Override
  public int compareTo(Object peer) throws ClassCastException {
    // TODO Auto-generated method stub
    return 0;
  }

}
