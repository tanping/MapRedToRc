package com.yahoo.ccdi.fetl.compiler;

import org.apache.hadoop.record.compiler.Consts;

import com.yahoo.ccdi.fetl.compiler.JRcType.JavaType;

public class JRcDouble extends JRcType{
  
  class JavaDouble extends JRcType.JavaType {
    
    JavaDouble() {
      super("double", "Double", "Double", "TypeID.RIOType.DOUBLE");
    }
    
    String getTypeIDObjectString() {
      return "org.apache.hadoop.record.meta.TypeID.DoubleTypeID";
    }

    void genHashCode(CodeBuffer cb, String fname) {
      String tmp = "Double.doubleToLongBits("+fname+")";
      cb.append(Consts.RIO_PREFIX + "ret = (int)("+tmp+"^("+tmp+">>>32));\n");
    }
    
    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
      cb.append("{\n");
      cb.append("if ("+l+"<8) {\n");
      cb.append("throw new java.io.IOException(\"Double is exactly 8 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append(s+"+=8; "+l+"-=8;\n");
      cb.append("}\n");
    }
    
    void genCompareBytes(CodeBuffer cb) {
      cb.append("{\n");
      cb.append("if (l1<8 || l2<8) {\n");
      cb.append("throw new java.io.IOException(\"Double is exactly 8 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append("double d1 = org.apache.hadoop.record.Utils.readDouble(b1, s1);\n");
      cb.append("double d2 = org.apache.hadoop.record.Utils.readDouble(b2, s2);\n");
      cb.append("if (d1 != d2) {\n");
      cb.append("return ((d1-d2) < 0) ? -1 : 0;\n");
      cb.append("}\n");
      cb.append("s1+=8; s2+=8; l1-=8; l2-=8;\n");
      cb.append("}\n");
    }
  }
  
  /** Creates a new instance of JDouble */
  public JRcDouble() {
    setJavaType(new JavaDouble());
  }
  
  String getSignature() {
    return "d";
  }


}
