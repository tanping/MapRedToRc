// File generated by hadoop record compiler. Do not edit.
package com.yahoo.ccdi.fetl;

public class ETLKey extends org.apache.hadoop.record.Record {
  private static final org.apache.hadoop.record.meta.RecordTypeInfo _rio_recTypeInfo;
  private static org.apache.hadoop.record.meta.RecordTypeInfo _rio_rtiFilter;
  private static int[] _rio_rtiFilterFields;
  static {
    _rio_recTypeInfo = new org.apache.hadoop.record.meta.RecordTypeInfo("ETLKey");
    _rio_recTypeInfo.addField("bcookie", org.apache.hadoop.record.meta.TypeID.BufferTypeID);
    _rio_recTypeInfo.addField("timestamp", org.apache.hadoop.record.meta.TypeID.LongTypeID);
  }
  
  private org.apache.hadoop.record.Buffer bcookie;
  private long timestamp;
  public ETLKey() { }
  public ETLKey(
    final org.apache.hadoop.record.Buffer bcookie,
    final long timestamp) {
    this.bcookie = bcookie;
    this.timestamp = timestamp;
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
  public void serialize(final org.apache.hadoop.record.RecordOutput _rio_a, final String _rio_tag)
  throws java.io.IOException {
    _rio_a.startRecord(this,_rio_tag);
    _rio_a.writeBuffer(bcookie,"bcookie");
    _rio_a.writeLong(timestamp,"timestamp");
    _rio_a.endRecord(this,_rio_tag);
  }
  private void deserializeWithoutFilter(final org.apache.hadoop.record.RecordInput _rio_a, final String _rio_tag)
  throws java.io.IOException {
    _rio_a.startRecord(_rio_tag);
    bcookie=_rio_a.readBuffer("bcookie");
    timestamp=_rio_a.readLong("timestamp");
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
        bcookie=_rio_a.readBuffer("bcookie");
      }
      else if (2 == _rio_rtiFilterFields[_rio_i]) {
        timestamp=_rio_a.readLong("timestamp");
      }
      else {
        java.util.ArrayList<org.apache.hadoop.record.meta.FieldTypeInfo> typeInfos = (java.util.ArrayList<org.apache.hadoop.record.meta.FieldTypeInfo>)(_rio_rtiFilter.getFieldTypeInfos());
        org.apache.hadoop.record.meta.Utils.skip(_rio_a, typeInfos.get(_rio_i).getFieldID(), typeInfos.get(_rio_i).getTypeID());
      }
    }
    _rio_a.endRecord(_rio_tag);
  }
  public int compareTo (final Object _rio_peer_) throws ClassCastException {
    if (!(_rio_peer_ instanceof ETLKey)) {
      throw new ClassCastException("Comparing different types of records.");
    }
    ETLKey _rio_peer = (ETLKey) _rio_peer_;
    int _rio_ret = 0;
    _rio_ret = bcookie.compareTo(_rio_peer.bcookie);
    if (_rio_ret != 0) return _rio_ret;
    _rio_ret = (timestamp == _rio_peer.timestamp)? 0 :((timestamp<_rio_peer.timestamp)?-1:1);
    if (_rio_ret != 0) return _rio_ret;
    return _rio_ret;
  }
  public boolean equals(final Object _rio_peer_) {
    if (!(_rio_peer_ instanceof ETLKey)) {
      return false;
    }
    if (_rio_peer_ == this) {
      return true;
    }
    ETLKey _rio_peer = (ETLKey) _rio_peer_;
    boolean _rio_ret = false;
    _rio_ret = bcookie.equals(_rio_peer.bcookie);
    if (!_rio_ret) return _rio_ret;
    _rio_ret = (timestamp==_rio_peer.timestamp);
    if (!_rio_ret) return _rio_ret;
    return _rio_ret;
  }
  public Object clone() throws CloneNotSupportedException {
    ETLKey _rio_other = new ETLKey();
    _rio_other.bcookie = (org.apache.hadoop.record.Buffer) this.bcookie.clone();
    _rio_other.timestamp = this.timestamp;
    return _rio_other;
  }
  public int hashCode() {
    int _rio_result = 17;
    int _rio_ret;
    _rio_ret = bcookie.hashCode();
    _rio_result = 37*_rio_result + _rio_ret;
    _rio_ret = (int) (timestamp^(timestamp>>>32));
    _rio_result = 37*_rio_result + _rio_ret;
    return _rio_result;
  }
  public static String signature() {
    return "LETLKey(Bl)";
  }
  public static class Comparator extends org.apache.hadoop.record.RecordComparator {
    public Comparator() {
      super(ETLKey.class);
    }
    static public int slurpRaw(byte[] b, int s, int l) {
      try {
        int os = s;
        {
          int i = org.apache.hadoop.record.Utils.readVInt(b, s);
          int z = org.apache.hadoop.record.Utils.getVIntSize(i);
          s += z+i; l -= (z+i);
        }
        {
          long i = org.apache.hadoop.record.Utils.readVLong(b, s);
          int z = org.apache.hadoop.record.Utils.getVIntSize(i);
          s+=z; l-=z;
        }
        return (os - s);
      } catch(java.io.IOException e) {
        throw new RuntimeException(e);
      }
    }
    static public int compareRaw(byte[] b1, int s1, int l1,
                                   byte[] b2, int s2, int l2) {
      try {
        int os1 = s1;
        {
          int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
          int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
          int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
          int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
          s1+=z1; s2+=z2; l1-=z1; l2-=z2;
          int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);
          if (r1 != 0) { return (r1<0)?-1:0; }
          s1+=i1; s2+=i2; l1-=i1; l1-=i2;
        }
        {
          long i1 = org.apache.hadoop.record.Utils.readVLong(b1, s1);
          long i2 = org.apache.hadoop.record.Utils.readVLong(b2, s2);
          if (i1 != i2) {
            return ((i1-i2) < 0) ? -1 : 0;
          }
          int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
          int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
          s1+=z1; s2+=z2; l1-=z1; l2-=z2;
        }
        return (os1 - s1);
      } catch(java.io.IOException e) {
        throw new RuntimeException(e);
      }
    }
    public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
      int ret = compareRaw(b1,s1,l1,b2,s2,l2);
      return (ret == -1)? -1 : ((ret==0)? 1 : 0);}
  }
  
  static {
    org.apache.hadoop.record.RecordComparator.define(ETLKey.class, new Comparator());
  }
}
