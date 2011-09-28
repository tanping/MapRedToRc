package com.yahoo.ccdi.fetl.compiler;

/**
 * Because JRcBoolean needs to extend JRcType, it can not extends another class
 * JBoolean at the same time. We have to copy the majority code of
 * org.apache.hadoop.record.compiler.JBoolean here in order to keep all the 
 * functionality of JBoolean and add genRCWriteMethod and genRCReadMethod to it
 * which both are inheritated from JRcType.
 */

import org.apache.hadoop.record.compiler.Consts;


public class JRcBoolean extends JRcType{
  
  class JavaBoolean extends JRcType.JavaType {
    
    JavaBoolean() {
      super("boolean", "Bool", "Boolean", "TypeID.RIOType.BOOL");
    }
    
    void genCompareTo(CodeBuffer cb, String fname, String other) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+" == "+other+")? 0 : ("+
          fname+"?1:-1);\n");
    }
    
    String getTypeIDObjectString() {
      return "org.apache.hadoop.record.meta.TypeID.BoolTypeID";
    }

    void genHashCode(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+")?0:1;\n");
    }
    
//    // In Binary format, boolean is written as byte. true = 1, false = 0
//    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
//      cb.append("{\n");
//      cb.append("if ("+l+"<1) {\n");
//      cb.append("throw new java.io.IOException(\"Boolean is exactly 1 byte."+
//                " Provided buffer is smaller.\");\n");
//      cb.append("}\n");
//      cb.append(s+"++; "+l+"--;\n");
//      cb.append("}\n");
//    }
//    
//    // In Binary format, boolean is written as byte. true = 1, false = 0
//    void genCompareBytes(CodeBuffer cb) {
//      cb.append("{\n");
//      cb.append("if (l1<1 || l2<1) {\n");
//      cb.append("throw new java.io.IOException(\"Boolean is exactly 1 byte."+
//                " Provided buffer is smaller.\");\n");
//      cb.append("}\n");
//      cb.append("if (b1[s1] != b2[s2]) {\n");
//      cb.append("return (b1[s1]<b2[s2])? -1 : 0;\n");
//      cb.append("}\n");
//      cb.append("s1++; s2++; l1--; l2--;\n");
//      cb.append("}\n");
//    }
  }
  

  /** Creates a new instance of JBoolean */
  public JRcBoolean() {
    setJavaType(new JavaBoolean());
  }
  
  String getSignature() {
    return "z";
  }


}
