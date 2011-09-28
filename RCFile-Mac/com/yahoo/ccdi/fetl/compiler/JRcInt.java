package com.yahoo.ccdi.fetl.compiler;

public class JRcInt extends JRcType {
  
  class JavaInt extends JavaType {
    
    JavaInt() {
      super("int", "Int", "Integer", "TypeID.RIOType.INT");
    }
    
    String getTypeIDObjectString() {
      return "org.apache.hadoop.record.meta.TypeID.IntTypeID";
    }

//    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
//      cb.append("{\n");
//      cb.append("int i = org.apache.hadoop.record.Utils.readVInt("+b+", "+s+");\n");
//      cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
//      cb.append(s+"+=z; "+l+"-=z;\n");
//      cb.append("}\n");
//    }
//    
//    void genCompareBytes(CodeBuffer cb) {
//      cb.append("{\n");
//      cb.append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
//      cb.append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
//      cb.append("if (i1 != i2) {\n");
//      cb.append("return ((i1-i2) < 0) ? -1 : 0;\n");
//      cb.append("}\n");
//      cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
//      cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
//      cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
//      cb.append("}\n");
//    }
  }


  /** Creates a new instance of JInt */
  public JRcInt() {
    setJavaType(new JavaInt());
  }
  
  String getSignature() {
    return "i";
  }


}
