package com.yahoo.ccdi.fetl.compiler;

import java.util.Map;

import org.apache.hadoop.record.compiler.Consts;

public class JRcVector extends JRcCompType{

  
  static private int level = 0;
  
  static private String getId(String id) { return id+getLevel(); }
  
  static private String getLevel() { return Integer.toString(level); }
  
  static private void incrLevel() { level++; }
  
  static private void decrLevel() { level--; }
  
  private JRcType type;
  
  class JavaVector extends JavaCompType {
    
    private JRcType.JavaType element;
    
    JavaVector(JRcType.JavaType t) {
      super("java.util.ArrayList<"+t.getWrapperType()+">",
            "Vector", "java.util.ArrayList<"+t.getWrapperType()+">",
            "TypeID.RIOType.VECTOR");
      element = t;
    }
    
    String getTypeIDObjectString() {
      return "new org.apache.hadoop.record.meta.VectorTypeID(" + 
      element.getTypeIDObjectString() + ")";
    }

    void genSetRTIFilter(CodeBuffer cb, Map<String, Integer> nestedStructMap) {
      element.genSetRTIFilter(cb, nestedStructMap);
    }

    void genCompareTo(CodeBuffer cb, String fname, String other) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId(Consts.RIO_PREFIX + "len1")+" = "+fname+
          ".size();\n");
      cb.append("int "+getId(Consts.RIO_PREFIX + "len2")+" = "+other+
          ".size();\n");
      cb.append("for(int "+getId(Consts.RIO_PREFIX + "vidx")+" = 0; "+
          getId(Consts.RIO_PREFIX + "vidx")+"<"+getId(Consts.RIO_PREFIX + "len1")+
          " && "+getId(Consts.RIO_PREFIX + "vidx")+"<"+
          getId(Consts.RIO_PREFIX + "len2")+"; "+
          getId(Consts.RIO_PREFIX + "vidx")+"++) {\n");
      cb.append(element.getType()+" "+getId(Consts.RIO_PREFIX + "e1")+
                " = "+fname+
                ".get("+getId(Consts.RIO_PREFIX + "vidx")+");\n");
      cb.append(element.getType()+" "+getId(Consts.RIO_PREFIX + "e2")+
                " = "+other+
                ".get("+getId(Consts.RIO_PREFIX + "vidx")+");\n");
      element.genCompareTo(cb, getId(Consts.RIO_PREFIX + "e1"), 
          getId(Consts.RIO_PREFIX + "e2"));
      cb.append("if (" + Consts.RIO_PREFIX + "ret != 0) { return " +
          Consts.RIO_PREFIX + "ret; }\n");
      cb.append("}\n");
      cb.append(Consts.RIO_PREFIX + "ret = ("+getId(Consts.RIO_PREFIX + "len1")+
          " - "+getId(Consts.RIO_PREFIX + "len2")+");\n");
      decrLevel();
      cb.append("}\n");
    }
    
    void genRCReadMethod(CodeBuffer cb, String fname, String bra_var_name, 
        String indx, boolean decl) {
      if (decl) {
        cb.append(getType()+" "+fname+";\n");
      }
      cb.append("{\n");
      incrLevel();
//      cb.append("org.apache.hadoop.record.Index "+
//          getId(Consts.RIO_PREFIX + "vidx")+" = " + 
//          Consts.RECORD_INPUT + ".startVector(\""+tag+"\");\n");
      cb.append("int " + getId(Consts.RIO_PREFIX + "len")
          + " = com.yahoo.ccdi.fetl.RcUtil.readInt("
          + bra_var_name
          +", "+ indx +"++);\n");
      cb.append(fname+"=new "+getType()+"();\n");
//      cb.append("for (; !"+getId(Consts.RIO_PREFIX + "vidx")+".done(); " + 
//          getId(Consts.RIO_PREFIX + "vidx")+".incr()) {\n");
      cb.append("for(int "+getId(Consts.RIO_PREFIX + "vidx")+" = 0; " + 
          getId(Consts.RIO_PREFIX + "vidx")+"<"+getId(Consts.RIO_PREFIX + "len")+
          "; "+getId(Consts.RIO_PREFIX + "vidx")+"++) {\n");
//      element.genReadMethod(cb, getId(Consts.RIO_PREFIX + "e"), 
//          getId(Consts.RIO_PREFIX + "e"), true);
      element.genRCReadMethod(cb, getId(Consts.RIO_PREFIX + "e"), bra_var_name, indx, true);
      cb.append(fname+".add("+getId(Consts.RIO_PREFIX + "e")+");\n");
      cb.append("}\n");
      //cb.append(Consts.RECORD_INPUT + ".endVector(\""+tag+"\");\n");
      decrLevel();
      cb.append("}\n");
    }
    
    void genRCWriteMethod(CodeBuffer cb, String fname, String indx) {
      cb.append("{\n");
      incrLevel();
      //cb.append(Consts.RECORD_OUTPUT + ".startVector("+fname+",\""+tag+"\");\n");
      cb.append("com.yahoo.ccdi.fetl.RcUtil.writeInt(this, "+ fname+".size(), "
          + indx +"++);\n");
      cb.append("int "+getId(Consts.RIO_PREFIX + "len")+" = "+fname+".size();\n");
      cb.append("for(int "+getId(Consts.RIO_PREFIX + "vidx")+" = 0; " + 
          getId(Consts.RIO_PREFIX + "vidx")+"<"+getId(Consts.RIO_PREFIX + "len")+
          "; "+getId(Consts.RIO_PREFIX + "vidx")+"++) {\n");
      cb.append(element.getType()+" "+getId(Consts.RIO_PREFIX + "e")+" = "+
          fname+".get("+getId(Consts.RIO_PREFIX + "vidx")+");\n");
//      element.genWriteMethod(cb, getId(Consts.RIO_PREFIX + "e"), 
//          getId(Consts.RIO_PREFIX + "e"));
      element.genRCWriteMethod(cb, getId(Consts.RIO_PREFIX + "e"), indx);
      cb.append("}\n");
      //cb.append(Consts.RECORD_OUTPUT + ".endVector("+fname+",\""+tag+"\");\n");
      cb.append("}\n");
      decrLevel();
    }
  }
  
  /** Creates a new instance of JVector */
  public JRcVector(JRcType t) {
    type = t;
    setJavaType(new JavaVector(t.getJavaType()));
  }
  
  String getSignature() {
    return "[" + type.getSignature() + "]";
  }
}
