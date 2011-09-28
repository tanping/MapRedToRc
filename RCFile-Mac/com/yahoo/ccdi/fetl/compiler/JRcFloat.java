package com.yahoo.ccdi.fetl.compiler;

import org.apache.hadoop.record.compiler.Consts;

public class JRcFloat extends JRcType{
  
  class JavaFloat extends JavaType {
    
    JavaFloat() {
      super("float", "Float", "Float", "TypeID.RIOType.FLOAT");
    }
    
    String getTypeIDObjectString() {
      return "org.apache.hadoop.record.meta.TypeID.FloatTypeID";
    }

    void genHashCode(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = Float.floatToIntBits("+fname+");\n");
    }
    
    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
      cb.append("{\n");
      cb.append("if ("+l+"<4) {\n");
      cb.append("throw new java.io.IOException(\"Float is exactly 4 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append(s+"+=4; "+l+"-=4;\n");
      cb.append("}\n");
    }
    
    void genCompareBytes(CodeBuffer cb) {
      cb.append("{\n");
      cb.append("if (l1<4 || l2<4) {\n");
      cb.append("throw new java.io.IOException(\"Float is exactly 4 bytes."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append("float f1 = org.apache.hadoop.record.Utils.readFloat(b1, s1);\n");
      cb.append("float f2 = org.apache.hadoop.record.Utils.readFloat(b2, s2);\n");
      cb.append("if (f1 != f2) {\n");
      cb.append("return ((f1-f2) < 0) ? -1 : 0;\n");
      cb.append("}\n");
      cb.append("s1+=4; s2+=4; l1-=4; l2-=4;\n");
      cb.append("}\n");
    }
  }


  /** Creates a new instance of JFloat */
  public JRcFloat() {
    setJavaType(new JavaFloat());
  }
  
  String getSignature() {
    return "f";
  }


}
