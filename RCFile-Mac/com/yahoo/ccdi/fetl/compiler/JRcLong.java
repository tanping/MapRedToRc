package com.yahoo.ccdi.fetl.compiler;

import org.apache.hadoop.record.compiler.Consts;

public class JRcLong extends JRcType {
  
  class JavaLong extends JavaType {
    
    JavaLong() {
      super("long", "Long", "Long", "TypeID.RIOType.LONG");
    }
    
    String getTypeIDObjectString() {
      return "org.apache.hadoop.record.meta.TypeID.LongTypeID";
    }

    void genHashCode(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = (int) ("+fname+"^("+
          fname+">>>32));\n");
    }
    
//    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
//      cb.append("{\n");
//      cb.append("long i = org.apache.hadoop.record.Utils.readVLong("+b+", "+s+");\n");
//      cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
//      cb.append(s+"+=z; "+l+"-=z;\n");
//      cb.append("}\n");
//    }
    
//    void genCompareBytes(CodeBuffer cb) {
//      cb.append("{\n");
//      cb.append("long i1 = org.apache.hadoop.record.Utils.readVLong(b1, s1);\n");
//      cb.append("long i2 = org.apache.hadoop.record.Utils.readVLong(b2, s2);\n");
//      cb.append("if (i1 != i2) {\n");
//      cb.append("return ((i1-i2) < 0) ? -1 : 0;\n");
//      cb.append("}\n");
//      cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
//      cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
//      cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
//      cb.append("}\n");
//    }
  }

  /** Creates a new instance of JLong */
  public JRcLong() {
    setJavaType(new JavaLong());
  }
  
  String getSignature() {
    return "l";
  }


}
