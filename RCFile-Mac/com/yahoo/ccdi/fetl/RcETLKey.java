// File generated by hadoop record compiler. Do not edit.
package com.yahoo.ccdi.fetl;

public class RcETLKey extends org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable {
  private static final org.apache.hadoop.record.meta.RecordTypeInfo _rio_recTypeInfo;
  private static org.apache.hadoop.record.meta.RecordTypeInfo _rio_rtiFilter;
  private static int[] _rio_rtiFilterFields;
  private static int writeIndx = 0;
  static {
    _rio_recTypeInfo = new org.apache.hadoop.record.meta.RecordTypeInfo("RcETLKey");
    _rio_recTypeInfo.addField("bcookie", org.apache.hadoop.record.meta.TypeID.BufferTypeID);
    _rio_recTypeInfo.addField("timestamp", org.apache.hadoop.record.meta.TypeID.LongTypeID);
  }
  
  private org.apache.hadoop.record.Buffer bcookie;
  private long timestamp;
  public RcETLKey() { }
  public RcETLKey(
    final org.apache.hadoop.record.Buffer bcookie,
    final long timestamp) {
    this.bcookie = bcookie;
    this.timestamp = timestamp;
    this.serialize();
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
  public org.apache.hadoop.record.Buffer getBcookie() {
    return bcookie;
  }
  public void setBcookie(final org.apache.hadoop.record.Buffer bcookie) {
    this.bcookie=bcookie;
  }
  public long getTimestamp() {
    return timestamp;
  }
  public void setTimestamp(final long timestamp) {
    this.timestamp=timestamp;
  }
  public void serialize() {
    int writeIndx = 0;
    try {
      com.yahoo.ccdi.fetl.RcUtil.writeBuffer(this, bcookie, writeIndx++);
      com.yahoo.ccdi.fetl.RcUtil.writeLong(this, timestamp, writeIndx++);
    } catch(java.io.IOException e) {
      e.printStackTrace();
    }
  }
  public void deserialize(org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable bra){
    int readIndx = 0;
    try {
      bcookie=com.yahoo.ccdi.fetl.RcUtil.readBuffer(bra, readIndx++);
      timestamp=com.yahoo.ccdi.fetl.RcUtil.readLong(bra, readIndx++);
    } catch(java.io.IOException e) {
      e.printStackTrace();
    }
  }
}
