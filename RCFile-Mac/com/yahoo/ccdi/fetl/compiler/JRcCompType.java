package com.yahoo.ccdi.fetl.compiler;

import org.apache.hadoop.record.compiler.Consts;

abstract public class JRcCompType extends JRcType{

  abstract class JavaCompType extends JavaType {
    JavaCompType(String type, String suffix, String wrapper, 
        String typeIDByteString) { 
      super(type, suffix, wrapper, typeIDByteString);
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
    
    void genClone(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "other."+fname+" = ("+getType()+") this."+
          fname+".clone();\n");
    }
  
    
  }


}
