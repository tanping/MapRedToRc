package com.yahoo.ccdi.fetl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MetricsValueType implements Writable{
  
  private long pv;
  private long adClicks;
  private long adViews;
  private long adClicksFilter;
  
  public long getPv() {
    return pv;
  }
  
  public void setPv(long pv) {
    this.pv = pv;
  }

  public long getAdClicks() {
    return adClicks;
  }
  
  public void setAdClicks(long adClicks) {
    this.adClicks = adClicks;
  }

  public long getAdViews() {
    return adViews;
  }

  public void setAdViews(long adViews) {
    this.adViews = adViews;
  }

  public long getAdClicksFilter() {
    return adClicksFilter;
  }

  public void setAdClicksFilter(long adClicksFilter) {
    this.adClicksFilter = adClicksFilter;
  }

  public MetricsValueType(long inPv, long inAdClicks, long inAdViews){
    this.pv = inPv;
    this.adClicks = inAdClicks;
    this.adViews = inAdViews;
  }
  
  public MetricsValueType(long inPv, long inAdClicks, long inAdViews, long inAdClicksFilter){
    this.pv = inAdClicks;
    this.adClicks = inAdClicks;
    this.adViews = inAdViews;
    this.adClicksFilter = inAdClicksFilter;
  }

  public MetricsValueType(){
          this(0,0,0,0);
  }

  public MetricsValueType add(MetricsValueType other) {
    this.pv += other.pv;
//    System.out.println("### Add pv = " + other.pv + "; pv = " + this.pv);
    this.adClicks += other.adClicks;
//    System.out.println("### Add pv = " + other.adClicks + "; adClick = "
//        + this.adClicks);
    this.adViews += other.adViews;
//    System.out.println("### Add pv = " + other.adViews + ";  adViews = "
//        + this.adViews);
    this.adClicksFilter += other.adClicksFilter;
//    System.out.println("### Add adClicksFilter = " + other.adClicksFilter
//        + "; adClickFilter = " + this.adClicksFilter);

    return this;
  }
  
  public void write(DataOutput out) throws IOException{
    out.writeLong(pv);
    out.writeLong(adClicks);
    out.writeLong(adViews);
    out.writeLong(adClicksFilter);
  } 

  public void readFields(DataInput in) throws IOException{
    pv=in.readLong();
    adClicks = in.readLong();
    adViews = in.readLong();
    adClicksFilter = in.readLong();
  }  
  public MetricsValueType read(DataInput in) throws IOException{
    MetricsValueType v = new MetricsValueType();
    v.readFields(in);
    return v;
  }
}
