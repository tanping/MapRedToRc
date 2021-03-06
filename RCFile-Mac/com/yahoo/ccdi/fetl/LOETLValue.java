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

//import com.yahoo.ccdi.fetl.ETLValue.Comparator;


public class LOETLValue extends Record{

  private static final org.apache.hadoop.record.meta.RecordTypeInfo _rio_recTypeInfo;
  private static org.apache.hadoop.record.meta.RecordTypeInfo _rio_rtiFilter;
  private static int[] _rio_rtiFilterFields;
  static {
    _rio_recTypeInfo = new org.apache.hadoop.record.meta.RecordTypeInfo("ETLValue");
    _rio_recTypeInfo.addField("filterTag", org.apache.hadoop.record.meta.TypeID.LongTypeID);
    _rio_recTypeInfo.addField("dhrTag", org.apache.hadoop.record.meta.TypeID.LongTypeID);
    _rio_recTypeInfo.addField("transformErrorTag", org.apache.hadoop.record.meta.TypeID.LongTypeID);
    _rio_recTypeInfo.addField("simpleFields", new org.apache.hadoop.record.meta.MapTypeID(org.apache.hadoop.record.meta.TypeID.StringTypeID, org.apache.hadoop.record.meta.TypeID.BufferTypeID));
    _rio_recTypeInfo.addField("mapFields", new org.apache.hadoop.record.meta.MapTypeID(org.apache.hadoop.record.meta.TypeID.StringTypeID, new org.apache.hadoop.record.meta.MapTypeID(org.apache.hadoop.record.meta.TypeID.StringTypeID, org.apache.hadoop.record.meta.TypeID.BufferTypeID)));
    _rio_recTypeInfo.addField("mapListFields", new org.apache.hadoop.record.meta.MapTypeID(org.apache.hadoop.record.meta.TypeID.StringTypeID, new org.apache.hadoop.record.meta.VectorTypeID(new org.apache.hadoop.record.meta.MapTypeID(org.apache.hadoop.record.meta.TypeID.StringTypeID, org.apache.hadoop.record.meta.TypeID.BufferTypeID))));
  }
  
  private long filterTag;
  private long dhrTag;
  private long transformErrorTag;
  private java.util.TreeMap<String,org.apache.hadoop.record.Buffer> simpleFields;
  private java.util.TreeMap<String,java.util.TreeMap<String,org.apache.hadoop.record.Buffer>> mapFields;
  private java.util.TreeMap<String,java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>> mapListFields;
  
  public LOETLValue() {}
  
  public LOETLValue(ETLValue eValue) {
    this.filterTag = eValue.getFilterTag();
    this.dhrTag = eValue.getDhrTag();
    this.transformErrorTag = eValue.getTransformErrorTag();
    this.simpleFields = eValue.getSimpleFields();
    this.mapFields = eValue.getMapFields();
    this.mapListFields = eValue.getMapListFields();
  }

  public static org.apache.hadoop.record.meta.RecordTypeInfo getTypeInfo() {
    return _rio_recTypeInfo;
  }
  public static void setTypeFilter(org.apache.hadoop.record.meta.RecordTypeInfo rti) {
    if (null == rti) return;
    _rio_rtiFilter = rti;
    _rio_rtiFilterFields = null;
  }
  private static void setupRtiFields()
  {
    if (null == _rio_rtiFilter) return;
    // we may already have done this
    if (null != _rio_rtiFilterFields) return;
    int _rio_i, _rio_j;
    _rio_rtiFilterFields = new int [_rio_rtiFilter.getFieldTypeInfos().size()];
    for (_rio_i=0; _rio_i<_rio_rtiFilterFields.length; _rio_i++) {
      _rio_rtiFilterFields[_rio_i] = 0;
    }
    java.util.Iterator<org.apache.hadoop.record.meta.FieldTypeInfo> _rio_itFilter = _rio_rtiFilter.getFieldTypeInfos().iterator();
    _rio_i=0;
    while (_rio_itFilter.hasNext()) {
      org.apache.hadoop.record.meta.FieldTypeInfo _rio_tInfoFilter = _rio_itFilter.next();
      java.util.Iterator<org.apache.hadoop.record.meta.FieldTypeInfo> _rio_it = _rio_recTypeInfo.getFieldTypeInfos().iterator();
      _rio_j=1;
      while (_rio_it.hasNext()) {
        org.apache.hadoop.record.meta.FieldTypeInfo _rio_tInfo = _rio_it.next();
        if (_rio_tInfo.equals(_rio_tInfoFilter)) {
          _rio_rtiFilterFields[_rio_i] = _rio_j;
          break;
        }
        _rio_j++;
      }
      _rio_i++;
    }
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
        //rout.writeBuffer(new Buffer(), "feedTag");
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
          //rout.writeBuffer(new Buffer(), "simpleFieldValueTag");
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
          //rout.writeBuffer(new Buffer(), "mapFieldValueTag");
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
          //rout.writeBuffer(new Buffer(), "listMapFiledValueTag");
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


  private void deserializeWithoutFilter(final org.apache.hadoop.record.RecordInput _rio_a, final String _rio_tag)
  throws java.io.IOException {

    _rio_a.startRecord(_rio_tag);
    filterTag=_rio_a.readLong("filterTag");
    dhrTag=_rio_a.readLong("dhrTag");
    transformErrorTag=_rio_a.readLong("transformErrorTag");
    {
      org.apache.hadoop.record.Index _rio_midx1 = _rio_a.startMap("simpleFields");
      simpleFields=new java.util.TreeMap<String,org.apache.hadoop.record.Buffer>();
      for (; !_rio_midx1.done(); _rio_midx1.incr()) {
        String _rio_k1;
        _rio_k1=_rio_a.readString("_rio_k1");
        org.apache.hadoop.record.Buffer _rio_v1;
        _rio_v1=_rio_a.readBuffer("_rio_v1");
        simpleFields.put(_rio_k1,_rio_v1);
      }
      _rio_a.endMap("simpleFields");
    }
    {
      org.apache.hadoop.record.Index _rio_midx1 = _rio_a.startMap("mapFields");
      mapFields=new java.util.TreeMap<String,java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>();
      for (; !_rio_midx1.done(); _rio_midx1.incr()) {
        String _rio_k1;
        _rio_k1=_rio_a.readString("_rio_k1");
        java.util.TreeMap<String,org.apache.hadoop.record.Buffer> _rio_v1;
        {
          org.apache.hadoop.record.Index _rio_midx2 = _rio_a.startMap("_rio_v1");
          _rio_v1=new java.util.TreeMap<String,org.apache.hadoop.record.Buffer>();
          for (; !_rio_midx2.done(); _rio_midx2.incr()) {
            String _rio_k2;
            _rio_k2=_rio_a.readString("_rio_k2");
            org.apache.hadoop.record.Buffer _rio_v2;
            _rio_v2=_rio_a.readBuffer("_rio_v2");
            _rio_v1.put(_rio_k2,_rio_v2);
          }
          _rio_a.endMap("_rio_v1");
        }
        mapFields.put(_rio_k1,_rio_v1);
      }
      _rio_a.endMap("mapFields");
    }
    {
      org.apache.hadoop.record.Index _rio_midx1 = _rio_a.startMap("mapListFields");
      mapListFields=new java.util.TreeMap<String,java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>>();
      for (; !_rio_midx1.done(); _rio_midx1.incr()) {
        String _rio_k1;
        _rio_k1=_rio_a.readString("_rio_k1");
        java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>> _rio_v1;
        {
          org.apache.hadoop.record.Index _rio_vidx1 = _rio_a.startVector("_rio_v1");
          _rio_v1=new java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>();
          for (; !_rio_vidx1.done(); _rio_vidx1.incr()) {
            java.util.TreeMap<String,org.apache.hadoop.record.Buffer> _rio_e1;
            {
              org.apache.hadoop.record.Index _rio_midx2 = _rio_a.startMap("_rio_e1");
              _rio_e1=new java.util.TreeMap<String,org.apache.hadoop.record.Buffer>();
              for (; !_rio_midx2.done(); _rio_midx2.incr()) {
                String _rio_k2;
                _rio_k2=_rio_a.readString("_rio_k2");
                org.apache.hadoop.record.Buffer _rio_v2;
                _rio_v2=_rio_a.readBuffer("_rio_v2");
                _rio_e1.put(_rio_k2,_rio_v2);
              }
              _rio_a.endMap("_rio_e1");
            }
            _rio_v1.add(_rio_e1);
          }
          _rio_a.endVector("_rio_v1");
        }
        mapListFields.put(_rio_k1,_rio_v1);
      }
      _rio_a.endMap("mapListFields");
    }
    _rio_a.endRecord(_rio_tag);
  
  }

public void deserialize(final org.apache.hadoop.record.RecordInput _rio_a, final String _rio_tag)
throws java.io.IOException {
  if (null == _rio_rtiFilter) {
    deserializeWithoutFilter(_rio_a, _rio_tag);
    return;
  }
  // if we're here, we need to read based on version info
  _rio_a.startRecord(_rio_tag);
  setupRtiFields();
  for (int _rio_i=0; _rio_i<_rio_rtiFilter.getFieldTypeInfos().size(); _rio_i++) {
    if (1 == _rio_rtiFilterFields[_rio_i]) {
      filterTag=_rio_a.readLong("filterTag");
    }
    else if (2 == _rio_rtiFilterFields[_rio_i]) {
      dhrTag=_rio_a.readLong("dhrTag");
    }
    else if (3 == _rio_rtiFilterFields[_rio_i]) {
      transformErrorTag=_rio_a.readLong("transformErrorTag");
    }
    else if (4 == _rio_rtiFilterFields[_rio_i]) {
      {
        org.apache.hadoop.record.Index _rio_midx1 = _rio_a.startMap("simpleFields");
        simpleFields=new java.util.TreeMap<String,org.apache.hadoop.record.Buffer>();
        for (; !_rio_midx1.done(); _rio_midx1.incr()) {
          String _rio_k1;
          _rio_k1=_rio_a.readString("_rio_k1");
          org.apache.hadoop.record.Buffer _rio_v1;
          _rio_v1=_rio_a.readBuffer("_rio_v1");
          simpleFields.put(_rio_k1,_rio_v1);
        }
        _rio_a.endMap("simpleFields");
      }
    }
    else if (5 == _rio_rtiFilterFields[_rio_i]) {
      {
        org.apache.hadoop.record.Index _rio_midx1 = _rio_a.startMap("mapFields");
        mapFields=new java.util.TreeMap<String,java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>();
        for (; !_rio_midx1.done(); _rio_midx1.incr()) {
          String _rio_k1;
          _rio_k1=_rio_a.readString("_rio_k1");
          java.util.TreeMap<String,org.apache.hadoop.record.Buffer> _rio_v1;
          {
            org.apache.hadoop.record.Index _rio_midx2 = _rio_a.startMap("_rio_v1");
            _rio_v1=new java.util.TreeMap<String,org.apache.hadoop.record.Buffer>();
            for (; !_rio_midx2.done(); _rio_midx2.incr()) {
              String _rio_k2;
              _rio_k2=_rio_a.readString("_rio_k2");
              org.apache.hadoop.record.Buffer _rio_v2;
              _rio_v2=_rio_a.readBuffer("_rio_v2");
              _rio_v1.put(_rio_k2,_rio_v2);
            }
            _rio_a.endMap("_rio_v1");
          }
          mapFields.put(_rio_k1,_rio_v1);
        }
        _rio_a.endMap("mapFields");
      }
    }
    else if (6 == _rio_rtiFilterFields[_rio_i]) {
      {
        org.apache.hadoop.record.Index _rio_midx1 = _rio_a.startMap("mapListFields");
        mapListFields=new java.util.TreeMap<String,java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>>();
        for (; !_rio_midx1.done(); _rio_midx1.incr()) {
          String _rio_k1;
          _rio_k1=_rio_a.readString("_rio_k1");
          java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>> _rio_v1;
          {
            org.apache.hadoop.record.Index _rio_vidx1 = _rio_a.startVector("_rio_v1");
            _rio_v1=new java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>();
            for (; !_rio_vidx1.done(); _rio_vidx1.incr()) {
              java.util.TreeMap<String,org.apache.hadoop.record.Buffer> _rio_e1;
              {
                org.apache.hadoop.record.Index _rio_midx2 = _rio_a.startMap("_rio_e1");
                _rio_e1=new java.util.TreeMap<String,org.apache.hadoop.record.Buffer>();
                for (; !_rio_midx2.done(); _rio_midx2.incr()) {
                  String _rio_k2;
                  _rio_k2=_rio_a.readString("_rio_k2");
                  org.apache.hadoop.record.Buffer _rio_v2;
                  _rio_v2=_rio_a.readBuffer("_rio_v2");
                  _rio_e1.put(_rio_k2,_rio_v2);
                }
                _rio_a.endMap("_rio_e1");
              }
              _rio_v1.add(_rio_e1);
            }
            _rio_a.endVector("_rio_v1");
          }
          mapListFields.put(_rio_k1,_rio_v1);
        }
        _rio_a.endMap("mapListFields");
      }
    }
    else {
      java.util.ArrayList<org.apache.hadoop.record.meta.FieldTypeInfo> typeInfos = (java.util.ArrayList<org.apache.hadoop.record.meta.FieldTypeInfo>)(_rio_rtiFilter.getFieldTypeInfos());
      org.apache.hadoop.record.meta.Utils.skip(_rio_a, typeInfos.get(_rio_i).getFieldID(), typeInfos.get(_rio_i).getTypeID());
    }
  }
  _rio_a.endRecord(_rio_tag);
}
//public int compareTo (final Object _rio_peer_) throws ClassCastException {
//  if (!(_rio_peer_ instanceof ETLValue)) {
//    throw new ClassCastException("Comparing different types of records.");
//  }
//  ETLValue _rio_peer = (ETLValue) _rio_peer_;
//  int _rio_ret = 0;
//  _rio_ret = (filterTag == _rio_peer.filterTag)? 0 :((filterTag<_rio_peer.filterTag)?-1:1);
//  if (_rio_ret != 0) return _rio_ret;
//  _rio_ret = (dhrTag == _rio_peer.dhrTag)? 0 :((dhrTag<_rio_peer.dhrTag)?-1:1);
//  if (_rio_ret != 0) return _rio_ret;
//  _rio_ret = (transformErrorTag == _rio_peer.transformErrorTag)? 0 :((transformErrorTag<_rio_peer.transformErrorTag)?-1:1);
//  if (_rio_ret != 0) return _rio_ret;
//  {
//    java.util.Set<String> _rio_set10 = simpleFields.keySet();
//    java.util.Set<String> _rio_set20 = _rio_peer.simpleFields.keySet();
//    java.util.Iterator<String> _rio_miter10 = _rio_set10.iterator();
//    java.util.Iterator<String> _rio_miter20 = _rio_set20.iterator();
//    for(; _rio_miter10.hasNext() && _rio_miter20.hasNext();) {
//      String _rio_k10 = _rio_miter10.next();
//      String _rio_k20 = _rio_miter20.next();
//      _rio_ret = _rio_k10.compareTo(_rio_k20);
//      if (_rio_ret != 0) { return _rio_ret; }
//    }
//    _rio_ret = (_rio_set10.size() - _rio_set20.size());
//  }
//  if (_rio_ret != 0) return _rio_ret;
//  {
//    java.util.Set<String> _rio_set10 = mapFields.keySet();
//    java.util.Set<String> _rio_set20 = _rio_peer.mapFields.keySet();
//    java.util.Iterator<String> _rio_miter10 = _rio_set10.iterator();
//    java.util.Iterator<String> _rio_miter20 = _rio_set20.iterator();
//    for(; _rio_miter10.hasNext() && _rio_miter20.hasNext();) {
//      String _rio_k10 = _rio_miter10.next();
//      String _rio_k20 = _rio_miter20.next();
//      _rio_ret = _rio_k10.compareTo(_rio_k20);
//      if (_rio_ret != 0) { return _rio_ret; }
//    }
//    _rio_ret = (_rio_set10.size() - _rio_set20.size());
//  }
//  if (_rio_ret != 0) return _rio_ret;
//  {
//    java.util.Set<String> _rio_set10 = mapListFields.keySet();
//    java.util.Set<String> _rio_set20 = _rio_peer.mapListFields.keySet();
//    java.util.Iterator<String> _rio_miter10 = _rio_set10.iterator();
//    java.util.Iterator<String> _rio_miter20 = _rio_set20.iterator();
//    for(; _rio_miter10.hasNext() && _rio_miter20.hasNext();) {
//      String _rio_k10 = _rio_miter10.next();
//      String _rio_k20 = _rio_miter20.next();
//      _rio_ret = _rio_k10.compareTo(_rio_k20);
//      if (_rio_ret != 0) { return _rio_ret; }
//    }
//    _rio_ret = (_rio_set10.size() - _rio_set20.size());
//  }
//  if (_rio_ret != 0) return _rio_ret;
//  return _rio_ret;
//}
//public boolean equals(final Object _rio_peer_) {
//  if (!(_rio_peer_ instanceof ETLValue)) {
//    return false;
//  }
//  if (_rio_peer_ == this) {
//    return true;
//  }
//  ETLValue _rio_peer = (ETLValue) _rio_peer_;
//  boolean _rio_ret = false;
//  _rio_ret = (filterTag==_rio_peer.filterTag);
//  if (!_rio_ret) return _rio_ret;
//  _rio_ret = (dhrTag==_rio_peer.dhrTag);
//  if (!_rio_ret) return _rio_ret;
//  _rio_ret = (transformErrorTag==_rio_peer.transformErrorTag);
//  if (!_rio_ret) return _rio_ret;
//  _rio_ret = simpleFields.equals(_rio_peer.simpleFields);
//  if (!_rio_ret) return _rio_ret;
//  _rio_ret = mapFields.equals(_rio_peer.mapFields);
//  if (!_rio_ret) return _rio_ret;
//  _rio_ret = mapListFields.equals(_rio_peer.mapListFields);
//  if (!_rio_ret) return _rio_ret;
//  return _rio_ret;
//}
//public Object clone() throws CloneNotSupportedException {
//  ETLValue _rio_other = new ETLValue();
//  _rio_other.filterTag = this.filterTag;
//  _rio_other.dhrTag = this.dhrTag;
//  _rio_other.transformErrorTag = this.transformErrorTag;
//  _rio_other.simpleFields = (java.util.TreeMap<String,org.apache.hadoop.record.Buffer>) this.simpleFields.clone();
//  _rio_other.mapFields = (java.util.TreeMap<String,java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>) this.mapFields.clone();
//  _rio_other.mapListFields = (java.util.TreeMap<String,java.util.ArrayList<java.util.TreeMap<String,org.apache.hadoop.record.Buffer>>>) this.mapListFields.clone();
//  return _rio_other;
//}
public int hashCode() {
  int _rio_result = 17;
  int _rio_ret;
  _rio_ret = (int) (filterTag^(filterTag>>>32));
  _rio_result = 37*_rio_result + _rio_ret;
  _rio_ret = (int) (dhrTag^(dhrTag>>>32));
  _rio_result = 37*_rio_result + _rio_ret;
  _rio_ret = (int) (transformErrorTag^(transformErrorTag>>>32));
  _rio_result = 37*_rio_result + _rio_ret;
  _rio_ret = simpleFields.hashCode();
  _rio_result = 37*_rio_result + _rio_ret;
  _rio_ret = mapFields.hashCode();
  _rio_result = 37*_rio_result + _rio_ret;
  _rio_ret = mapListFields.hashCode();
  _rio_result = 37*_rio_result + _rio_ret;
  return _rio_result;
}
public static String signature() {
  return "LETLValue(lll{sB}{s{sB}}{s[{sB}]})";
}
//public static class Comparator extends org.apache.hadoop.record.RecordComparator {
//  public Comparator() {
//    super(ETLValue.class);
//  }
//  static public int slurpRaw(byte[] b, int s, int l) {
//    try {
//      int os = s;
//      {
//        long i = org.apache.hadoop.record.Utils.readVLong(b, s);
//        int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//        s+=z; l-=z;
//      }
//      {
//        long i = org.apache.hadoop.record.Utils.readVLong(b, s);
//        int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//        s+=z; l-=z;
//      }
//      {
//        long i = org.apache.hadoop.record.Utils.readVLong(b, s);
//        int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//        s+=z; l-=z;
//      }
//      {
//        int mi1 = org.apache.hadoop.record.Utils.readVInt(b, s);
//        int mz1 = org.apache.hadoop.record.Utils.getVIntSize(mi1);
//        s+=mz1; l-=mz1;
//        for (int midx1 = 0; midx1 < mi1; midx1++) {{
//            int i = org.apache.hadoop.record.Utils.readVInt(b, s);
//            int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//            s+=(z+i); l-= (z+i);
//          }
//          {
//            int i = org.apache.hadoop.record.Utils.readVInt(b, s);
//            int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//            s += z+i; l -= (z+i);
//          }
//        }
//      }
//      {
//        int mi1 = org.apache.hadoop.record.Utils.readVInt(b, s);
//        int mz1 = org.apache.hadoop.record.Utils.getVIntSize(mi1);
//        s+=mz1; l-=mz1;
//        for (int midx1 = 0; midx1 < mi1; midx1++) {{
//            int i = org.apache.hadoop.record.Utils.readVInt(b, s);
//            int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//            s+=(z+i); l-= (z+i);
//          }
//          {
//            int mi2 = org.apache.hadoop.record.Utils.readVInt(b, s);
//            int mz2 = org.apache.hadoop.record.Utils.getVIntSize(mi2);
//            s+=mz2; l-=mz2;
//            for (int midx2 = 0; midx2 < mi2; midx2++) {{
//                int i = org.apache.hadoop.record.Utils.readVInt(b, s);
//                int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                s+=(z+i); l-= (z+i);
//              }
//              {
//                int i = org.apache.hadoop.record.Utils.readVInt(b, s);
//                int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                s += z+i; l -= (z+i);
//              }
//            }
//          }
//        }
//      }
//      {
//        int mi1 = org.apache.hadoop.record.Utils.readVInt(b, s);
//        int mz1 = org.apache.hadoop.record.Utils.getVIntSize(mi1);
//        s+=mz1; l-=mz1;
//        for (int midx1 = 0; midx1 < mi1; midx1++) {{
//            int i = org.apache.hadoop.record.Utils.readVInt(b, s);
//            int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//            s+=(z+i); l-= (z+i);
//          }
//          {
//            int vi1 = org.apache.hadoop.record.Utils.readVInt(b, s);
//            int vz1 = org.apache.hadoop.record.Utils.getVIntSize(vi1);
//            s+=vz1; l-=vz1;
//            for (int vidx1 = 0; vidx1 < vi1; vidx1++){
//              int mi2 = org.apache.hadoop.record.Utils.readVInt(b, s);
//              int mz2 = org.apache.hadoop.record.Utils.getVIntSize(mi2);
//              s+=mz2; l-=mz2;
//              for (int midx2 = 0; midx2 < mi2; midx2++) {{
//                  int i = org.apache.hadoop.record.Utils.readVInt(b, s);
//                  int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                  s+=(z+i); l-= (z+i);
//                }
//                {
//                  int i = org.apache.hadoop.record.Utils.readVInt(b, s);
//                  int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                  s += z+i; l -= (z+i);
//                }
//              }
//            }
//          }
//        }
//      }
//      return (os - s);
//    } catch(java.io.IOException e) {
//      throw new RuntimeException(e);
//    }
//  }
//  static public int compareRaw(byte[] b1, int s1, int l1,
//                                 byte[] b2, int s2, int l2) {
//    try {
//      int os1 = s1;
//      {
//        long i1 = org.apache.hadoop.record.Utils.readVLong(b1, s1);
//        long i2 = org.apache.hadoop.record.Utils.readVLong(b2, s2);
//        if (i1 != i2) {
//          return ((i1-i2) < 0) ? -1 : 0;
//        }
//        int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
//        int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
//        s1+=z1; s2+=z2; l1-=z1; l2-=z2;
//      }
//      {
//        long i1 = org.apache.hadoop.record.Utils.readVLong(b1, s1);
//        long i2 = org.apache.hadoop.record.Utils.readVLong(b2, s2);
//        if (i1 != i2) {
//          return ((i1-i2) < 0) ? -1 : 0;
//        }
//        int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
//        int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
//        s1+=z1; s2+=z2; l1-=z1; l2-=z2;
//      }
//      {
//        long i1 = org.apache.hadoop.record.Utils.readVLong(b1, s1);
//        long i2 = org.apache.hadoop.record.Utils.readVLong(b2, s2);
//        if (i1 != i2) {
//          return ((i1-i2) < 0) ? -1 : 0;
//        }
//        int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
//        int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
//        s1+=z1; s2+=z2; l1-=z1; l2-=z2;
//      }
//      {
//        int mi11 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//        int mi21 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//        int mz11 = org.apache.hadoop.record.Utils.getVIntSize(mi11);
//        int mz21 = org.apache.hadoop.record.Utils.getVIntSize(mi21);
//        s1+=mz11; s2+=mz21; l1-=mz11; l2-=mz21;
//        for (int midx1 = 0; midx1 < mi11 && midx1 < mi21; midx1++) {{
//            int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//            int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//            int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
//            int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
//            s1+=z1; s2+=z2; l1-=z1; l2-=z2;
//            int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);
//            if (r1 != 0) { return (r1<0)?-1:0; }
//            s1+=i1; s2+=i2; l1-=i1; l1-=i2;
//          }
//          {
//            int i = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//            int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//            s1 += z+i; l1 -= (z+i);
//          }
//          {
//            int i = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//            int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//            s2 += z+i; l2 -= (z+i);
//          }
//        }
//        if (mi11 != mi21) { return (mi11<mi21)?-1:0; }
//      }
//      {
//        int mi11 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//        int mi21 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//        int mz11 = org.apache.hadoop.record.Utils.getVIntSize(mi11);
//        int mz21 = org.apache.hadoop.record.Utils.getVIntSize(mi21);
//        s1+=mz11; s2+=mz21; l1-=mz11; l2-=mz21;
//        for (int midx1 = 0; midx1 < mi11 && midx1 < mi21; midx1++) {{
//            int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//            int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//            int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
//            int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
//            s1+=z1; s2+=z2; l1-=z1; l2-=z2;
//            int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);
//            if (r1 != 0) { return (r1<0)?-1:0; }
//            s1+=i1; s2+=i2; l1-=i1; l1-=i2;
//          }
//          {
//            int mi2 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//            int mz2 = org.apache.hadoop.record.Utils.getVIntSize(mi2);
//            s1+=mz2; l1-=mz2;
//            for (int midx2 = 0; midx2 < mi2; midx2++) {{
//                int i = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//                int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                s1+=(z+i); l1-= (z+i);
//              }
//              {
//                int i = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//                int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                s1 += z+i; l1 -= (z+i);
//              }
//            }
//          }
//          {
//            int mi2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//            int mz2 = org.apache.hadoop.record.Utils.getVIntSize(mi2);
//            s2+=mz2; l2-=mz2;
//            for (int midx2 = 0; midx2 < mi2; midx2++) {{
//                int i = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//                int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                s2+=(z+i); l2-= (z+i);
//              }
//              {
//                int i = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//                int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                s2 += z+i; l2 -= (z+i);
//              }
//            }
//          }
//        }
//        if (mi11 != mi21) { return (mi11<mi21)?-1:0; }
//      }
//      {
//        int mi11 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//        int mi21 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//        int mz11 = org.apache.hadoop.record.Utils.getVIntSize(mi11);
//        int mz21 = org.apache.hadoop.record.Utils.getVIntSize(mi21);
//        s1+=mz11; s2+=mz21; l1-=mz11; l2-=mz21;
//        for (int midx1 = 0; midx1 < mi11 && midx1 < mi21; midx1++) {{
//            int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//            int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//            int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
//            int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
//            s1+=z1; s2+=z2; l1-=z1; l2-=z2;
//            int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);
//            if (r1 != 0) { return (r1<0)?-1:0; }
//            s1+=i1; s2+=i2; l1-=i1; l1-=i2;
//          }
//          {
//            int vi1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//            int vz1 = org.apache.hadoop.record.Utils.getVIntSize(vi1);
//            s1+=vz1; l1-=vz1;
//            for (int vidx1 = 0; vidx1 < vi1; vidx1++){
//              int mi2 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//              int mz2 = org.apache.hadoop.record.Utils.getVIntSize(mi2);
//              s1+=mz2; l1-=mz2;
//              for (int midx2 = 0; midx2 < mi2; midx2++) {{
//                  int i = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//                  int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                  s1+=(z+i); l1-= (z+i);
//                }
//                {
//                  int i = org.apache.hadoop.record.Utils.readVInt(b1, s1);
//                  int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                  s1 += z+i; l1 -= (z+i);
//                }
//              }
//            }
//          }
//          {
//            int vi1 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//            int vz1 = org.apache.hadoop.record.Utils.getVIntSize(vi1);
//            s2+=vz1; l2-=vz1;
//            for (int vidx1 = 0; vidx1 < vi1; vidx1++){
//              int mi2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//              int mz2 = org.apache.hadoop.record.Utils.getVIntSize(mi2);
//              s2+=mz2; l2-=mz2;
//              for (int midx2 = 0; midx2 < mi2; midx2++) {{
//                  int i = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//                  int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                  s2+=(z+i); l2-= (z+i);
//                }
//                {
//                  int i = org.apache.hadoop.record.Utils.readVInt(b2, s2);
//                  int z = org.apache.hadoop.record.Utils.getVIntSize(i);
//                  s2 += z+i; l2 -= (z+i);
//                }
//              }
//            }
//          }
//        }
//        if (mi11 != mi21) { return (mi11<mi21)?-1:0; }
//      }
//      return (os1 - s1);
//    } catch(java.io.IOException e) {
//      throw new RuntimeException(e);
//    }
//  }
//  public int compare(byte[] b1, int s1, int l1,
//                       byte[] b2, int s2, int l2) {
//    int ret = compareRaw(b1,s1,l1,b2,s2,l2);
//    return (ret == -1)? -1 : ((ret==0)? 1 : 0);}
//}
//
//static {
//  org.apache.hadoop.record.RecordComparator.define(ETLValue.class, new Comparator());
//}

@Override
public int compareTo(Object peer) throws ClassCastException {
  // TODO Auto-generated method stub
  return 0;
}
}

