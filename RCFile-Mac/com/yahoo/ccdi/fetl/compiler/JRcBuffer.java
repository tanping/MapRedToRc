package com.yahoo.ccdi.fetl.compiler;

import org.apache.hadoop.record.compiler.Consts;

public class JRcBuffer extends JRcCompType {
  
  class JavaBuffer extends JavaCompType {
  JavaBuffer() {
    super("org.apache.hadoop.record.Buffer", "Buffer", 
        "org.apache.hadoop.record.Buffer", "TypeID.RIOType.BUFFER");
  }
  
  String getTypeIDObjectString() {
    return "org.apache.hadoop.record.meta.TypeID.BufferTypeID";
  }

  void genCompareTo(CodeBuffer cb, String fname, String other) {
    cb.append(Consts.RIO_PREFIX + "ret = "+fname+".compareTo("+other+");\n");
  }
  
  void genEquals(CodeBuffer cb, String fname, String peer) {
    cb.append(Consts.RIO_PREFIX + "ret = "+fname+".equals("+peer+");\n");
  }
  
  void genHashCode(CodeBuffer cb, String fname) {
    cb.append(Consts.RIO_PREFIX + "ret = "+fname+".hashCode();\n");
  }
  
//  void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
//    cb.append("{\n");
//    cb.append("int i = org.apache.hadoop.record.Utils.readVInt("+
//              b+", "+s+");\n");
//    cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
//    cb.append(s+" += z+i; "+l+" -= (z+i);\n");
//    cb.append("}\n");
//  }
  
//  void genCompareBytes(CodeBuffer cb) {
//    cb.append("{\n");
//    cb.append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
//    cb.append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
//    cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
//    cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
//    cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
//    cb.append("int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);\n");
//    cb.append("if (r1 != 0) { return (r1<0)?-1:0; }\n");
//    cb.append("s1+=i1; s2+=i2; l1-=i1; l1-=i2;\n");
//    cb.append("}\n");
//  }
  }
  /** Creates a new instance of JBuffer */
  public JRcBuffer() {
    setJavaType(new JavaBuffer());
  }
  
  String getSignature() {
    return "B";
  }
}
