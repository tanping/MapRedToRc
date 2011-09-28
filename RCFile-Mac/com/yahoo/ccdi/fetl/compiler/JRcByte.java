package com.yahoo.ccdi.fetl.compiler;

/**
 * Because JRcByte needs to extend JRcType, it can not extends another class
 * JByte at the same time. We have to copy the majority code of
 * org.apache.hadoop.record.compiler.JByte here in order to keep all the 
 * functionality of JByte and add genRCWriteMethod and genRCReadMethod to it
 * which both are inheritated from JRcType.
 */

public class JRcByte extends JRcType{
  
  class JavaByte extends JavaType {
    JavaByte() {
      super("byte", "Byte", "Byte", "TypeID.RIOType.BYTE");
    }
    
    String getTypeIDObjectString() {
      return "org.apache.hadoop.record.meta.TypeID.ByteTypeID";
    }

//    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
//      cb.append("{\n");
//      cb.append("if ("+l+"<1) {\n");
//      cb.append("throw new java.io.IOException(\"Byte is exactly 1 byte."+
//                " Provided buffer is smaller.\");\n");
//      cb.append("}\n");
//      cb.append(s+"++; "+l+"--;\n");
//      cb.append("}\n");
//    }
//    
//    void genCompareBytes(CodeBuffer cb) {
//      cb.append("{\n");
//      cb.append("if (l1<1 || l2<1) {\n");
//      cb.append("throw new java.io.IOException(\"Byte is exactly 1 byte."+
//                " Provided buffer is smaller.\");\n");
//      cb.append("}\n");
//      cb.append("if (b1[s1] != b2[s2]) {\n");
//      cb.append("return (b1[s1]<b2[s2])?-1:0;\n");
//      cb.append("}\n");
//      cb.append("s1++; s2++; l1--; l2--;\n");
//      cb.append("}\n");
//    }
  
  }

  public JRcByte() {
    setJavaType(new JavaByte());
  }
  
  String getSignature() {
    return "b";
  }
}
