package com.yahoo.ccdi.fetl;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.BinaryRecordOutput;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.record.Utils;

public class RcUtil {

  private static final int B10 =    Integer.parseInt("10000000", 2);
  private static final int B110 =   Integer.parseInt("11000000", 2);
  private static final int B1110 =  Integer.parseInt("11100000", 2);
  private static final int B11110 = Integer.parseInt("11110000", 2);
  private static final int B11 =    Integer.parseInt("11000000", 2);
  private static final int B111 =   Integer.parseInt("11100000", 2);
  private static final int B1111 =  Integer.parseInt("11110000", 2);
  private static final int B11111 = Integer.parseInt("11111000", 2);
  
  // Writes
  public static void writeByte(BytesRefArrayWritable ba, byte b, int index)
      throws IOException {
    ba.set(index, new BytesRefWritable(b));
  }
    
  public static void writeBool(BytesRefArrayWritable ba, boolean b, int index) 
    throws IOException {
    byte[] byArray = WritableUtils.toByteArray(new BooleanWritable(b));
    ba.set(index, new BytesRefWritable(byArray));
  }
    
  public static void writeInt(BytesRefArrayWritable ba, int i, int index) 
    throws IOException {
    byte[] byArray = WritableUtils.toByteArray(new IntWritable(i));
    ba.set(index, new BytesRefWritable(byArray));
  }
    
  public static void writeLong(BytesRefArrayWritable ba, long l, int index) 
    throws IOException {
    byte[] byArray = WritableUtils.toByteArray(new LongWritable(l));
    ba.set(index, new BytesRefWritable(byArray));
  }
  
  public static void writeFloat(BytesRefArrayWritable ba, float f, int index) 
    throws IOException {
    byte[] byArray = WritableUtils.toByteArray(new FloatWritable(f));
    ba.set(index, new BytesRefWritable(byArray));
  }
    
  public static void writeDouble(BytesRefArrayWritable ba, double d, int index) 
    throws IOException {
    byte[] byArray = WritableUtils.toByteArray(new DoubleWritable(d));
    ba.set(index, new BytesRefWritable(byArray));
  }
    
  public static void writeString(BytesRefArrayWritable ba, String s, int index) 
    throws IOException {
      byte[] utf8Bytes = s.getBytes("UTF8");
      ba.set(index, new BytesRefWritable(utf8Bytes));

  }
    
  public static void writeBuffer(BytesRefArrayWritable ba, Buffer buf, int index)
    throws IOException {
    byte[] barr = buf.get();
    ba.set(index, new BytesRefWritable(barr));
  }
  
  /////////// Reads
  public static byte readByte(BytesRefArrayWritable ba, int index) throws IOException {
    BytesRefWritable brw = ba.get(index);
    return brw.getBytesCopy()[0];
  }
    
  public static boolean readBool(BytesRefArrayWritable ba, int index) throws IOException {
    //return in.readBoolean();
    // get byte array
    BytesRefWritable brw = ba.get(index);
    byte[] bytes = brw.getBytesCopy();
    // get DataInput
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes); 
    DataInputStream dis = new DataInputStream(bis); 
    return dis.readBoolean();
  }
    
  public static int readInt(BytesRefArrayWritable ba, int index) throws IOException {
    //return Utils.readVInt(in);
    // get byte array
    BytesRefWritable brw = ba.get(index);
    byte[] bytes = brw.getBytesCopy();
    // get DataInput
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes); 
    DataInputStream dis = new DataInputStream(bis); 
    // readInt
    //Utils.readVInt(dis);
    return dis.readInt();
  }
    
  public static long readLong(BytesRefArrayWritable ba, int index) throws IOException {
    //return Utils.readVLong(in);
    BytesRefWritable brw = ba.get(index);
    byte[] bytes = brw.getBytesCopy();
    // get DataInput
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes); 
    DataInputStream dis = new DataInputStream(bis); 
    return dis.readLong();
  }
    
  public static float readFloat(BytesRefArrayWritable ba, int index) throws IOException {
    //return in.readFloat();
    BytesRefWritable brw = ba.get(index);
    byte[] bytes = brw.getBytesCopy();
    // get DataInput
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes); 
    DataInputStream dis = new DataInputStream(bis); 
    return dis.readFloat();
  }
    
  public static double readDouble(BytesRefArrayWritable ba, int index) throws IOException {
    //return in.readDouble();
    BytesRefWritable brw = ba.get(index);
    byte[] bytes = brw.getBytesCopy();
    // get DataInput
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes); 
    DataInputStream dis = new DataInputStream(bis); 
    return dis.readDouble();
  }
    
  public static Buffer readBuffer(BytesRefArrayWritable ba, int index) throws IOException {
//    final int len = Utils.readVInt(in);
//    final byte[] barr = new byte[len];
//    in.readFully(barr);
//    return new Buffer(barr);
    BytesRefWritable brw = ba.get(index);
    byte[] bytes = brw.getBytesCopy();
    return new Buffer(bytes);
  }
    
  public static String readString(BytesRefArrayWritable ba, int index) throws IOException {
    BytesRefWritable brw = ba.get(index);
    byte[] bytes = brw.getBytesCopy();
    return new String(bytes);
  }
  
  private static void checkB10(int b) throws IOException {
    if ((b & B11) != B10) {
      throw new IOException("Invalid UTF-8 representation.");
    }
  }
  
  private static int utf8ToCodePoint(int b1, int b2, int b3, int b4) {
    int cpt = 0;
    cpt = (((b1 & ~B11111) << 18) |
           ((b2 & ~B11) << 12) |
           ((b3 & ~B11) << 6) |
           (b4 & ~B11));
    return cpt;
  }
  
  private static int utf8ToCodePoint(int b1, int b2, int b3) {
    int cpt = 0;
    cpt = (((b1 & ~B1111) << 12) | ((b2 & ~B11) << 6) | (b3 & ~B11));
    return cpt;
  }
  
  private static int utf8ToCodePoint(int b1, int b2) {
    int cpt = 0;
    cpt = (((b1 & ~B111) << 6) | (b2 & ~B11));
    return cpt;
  }
  
  static boolean isValidCodePoint(int cpt) {
    return !((cpt > 0x10FFFF) ||
             (cpt >= 0xD800 && cpt <= 0xDFFF) ||
             (cpt >= 0xFFFE && cpt <=0xFFFF));
  }
//  public void startRecord(Record r, String tag) throws IOException {}
//    
//  public void endRecord(Record r, String tag) throws IOException {}
//    
//  public void startVector(ArrayList v, String tag) throws IOException {
//    writeInt(v.size(), tag);
//  }
//    
//  public void endVector(ArrayList v, String tag) throws IOException {}
//    
//  public void startMap(TreeMap v, String tag) throws IOException {
//    writeInt(v.size(), tag);
//  }
//    
//  public void endMap(TreeMap v, String tag) throws IOException {}
    

}
