package com.yahoo.ccdi.fetl.compiler;

import java.util.Map;

import org.apache.hadoop.record.compiler.Consts;

public class JRcMap extends JRcCompType {
  
  static private int level = 0;
  
  static private String getLevel() { return Integer.toString(level); }
  
  static private void incrLevel() { level++; }
  
  static private void decrLevel() { level--; }
  
  static private String getId(String id) { return id+getLevel(); }
  
  private JRcType keyType;
  private JRcType valueType;
  
  class JavaMap extends JavaCompType {
    
    JRcType.JavaType key;
    JRcType.JavaType value;
    
    JavaMap(JRcType.JavaType key, JRcType.JavaType value) {
      super("java.util.TreeMap<"+key.getWrapperType()+","+value.getWrapperType()+">",
            "Map",
            "java.util.TreeMap<"+key.getWrapperType()+","+value.getWrapperType()+">",
            "TypeID.RIOType.MAP");
      this.key = key;
      this.value = value;
    }
    
    String getTypeIDObjectString() {
      return "new org.apache.hadoop.record.meta.MapTypeID(" + 
        key.getTypeIDObjectString() + ", " + 
        value.getTypeIDObjectString() + ")";
    }

//    void genSetRTIFilter(CodeBuffer cb, Map<String, Integer> nestedStructMap) {
//      key.genSetRTIFilter(cb, nestedStructMap);
//      value.genSetRTIFilter(cb, nestedStructMap);
//    }

    void genCompareTo(CodeBuffer cb, String fname, String other) {
      String setType = "java.util.Set<"+key.getWrapperType()+"> ";
      String iterType = "java.util.Iterator<"+key.getWrapperType()+"> ";
      cb.append("{\n");
      cb.append(setType+getId(Consts.RIO_PREFIX + "set1")+" = "+
          fname+".keySet();\n");
      cb.append(setType+getId(Consts.RIO_PREFIX + "set2")+" = "+
          other+".keySet();\n");
      cb.append(iterType+getId(Consts.RIO_PREFIX + "miter1")+" = "+
                getId(Consts.RIO_PREFIX + "set1")+".iterator();\n");
      cb.append(iterType+getId(Consts.RIO_PREFIX + "miter2")+" = "+
                getId(Consts.RIO_PREFIX + "set2")+".iterator();\n");
      cb.append("for(; "+getId(Consts.RIO_PREFIX + "miter1")+".hasNext() && "+
                getId(Consts.RIO_PREFIX + "miter2")+".hasNext();) {\n");
      cb.append(key.getType()+" "+getId(Consts.RIO_PREFIX + "k1")+
                " = "+getId(Consts.RIO_PREFIX + "miter1")+".next();\n");
      cb.append(key.getType()+" "+getId(Consts.RIO_PREFIX + "k2")+
                " = "+getId(Consts.RIO_PREFIX + "miter2")+".next();\n");
      key.genCompareTo(cb, getId(Consts.RIO_PREFIX + "k1"), 
          getId(Consts.RIO_PREFIX + "k2"));
      cb.append("if (" + Consts.RIO_PREFIX + "ret != 0) { return " + 
          Consts.RIO_PREFIX + "ret; }\n");
      cb.append("}\n");
      cb.append(Consts.RIO_PREFIX + "ret = ("+getId(Consts.RIO_PREFIX + "set1")+
          ".size() - "+getId(Consts.RIO_PREFIX + "set2")+".size());\n");
      cb.append("}\n");
    }
    
@Override
    void genRCReadMethod(CodeBuffer cb, String fname, String bra_var_name, 
        String indx, boolean decl) {
      if (decl) {
        cb.append(getType() + " " + fname + ";\n");
      }
      cb.append("{\n");
      incrLevel();
      // cb.append("org.apache.hadoop.record.Index " +
      // getId(Consts.RIO_PREFIX + "midx")+" = " +
      // Consts.RECORD_INPUT + ".startMap(\""+tag+"\");\n");
      cb.append("int " + getId(Consts.RIO_PREFIX + "midx")
          + "= com.yahoo.ccdi.fetl.RcUtil.readInt(" + bra_var_name + ", "
          + indx + "++);\n");
      cb.append(fname + "=new " + getType() + "();\n");
      // cb.append("for (; !"+getId(Consts.RIO_PREFIX + "midx")+".done(); "+
      // getId(Consts.RIO_PREFIX + "midx")+".incr()) {\n");
      cb.append("for ( int " + getId(Consts.RIO_PREFIX + "tmp") + " = 0; "
          + getId(Consts.RIO_PREFIX + "tmp") + "< "
          + getId(Consts.RIO_PREFIX + "midx") + "; "
          + getId(Consts.RIO_PREFIX + "tmp") + "++) {\n");
      // key.genReadMethod(cb, getId(Consts.RIO_PREFIX + "k"),
      // getId(Consts.RIO_PREFIX + "k"), true);
      key.genRCReadMethod(cb, getId(Consts.RIO_PREFIX + "k"), bra_var_name,
          indx, true);
      // value.genReadMethod(cb, getId(Consts.RIO_PREFIX + "v"),
      // getId(Consts.RIO_PREFIX + "v"), true);
      value.genRCReadMethod(cb, getId(Consts.RIO_PREFIX + "v"), bra_var_name,
          indx, true);
      cb.append(fname + ".put(" + getId(Consts.RIO_PREFIX + "k") + ","
          + getId(Consts.RIO_PREFIX + "v") + ");\n");
      cb.append("}\n");
      // cb.append(Consts.RECORD_INPUT + ".endMap(\""+tag+"\");\n");
      decrLevel();
      cb.append("}\n");
    }
    
@Override
    void genRCWriteMethod(CodeBuffer cb, String fname, String indx) {
      String setType = "java.util.Set<java.util.Map.Entry<"
          + key.getWrapperType() + "," + value.getWrapperType() + ">> ";
      String entryType = "java.util.Map.Entry<" + key.getWrapperType() + ","
          + value.getWrapperType() + "> ";
      String iterType = "java.util.Iterator<java.util.Map.Entry<"
          + key.getWrapperType() + "," + value.getWrapperType() + ">> ";
      cb.append("{\n");
      incrLevel();
//      cb.append(Consts.RECORD_OUTPUT + ".startMap(" + fname + ",\"" + tag
//          + "\");\n");
      cb.append("com.yahoo.ccdi.fetl.RcUtil.writeInt(this, "+ fname+".size(), "
          + indx +"++);\n");
      cb.append(setType + getId(Consts.RIO_PREFIX + "es") + " = " + fname
          + ".entrySet();\n");
      cb.append("for(" + iterType + getId(Consts.RIO_PREFIX + "midx") + " = "
          + getId(Consts.RIO_PREFIX + "es") + ".iterator(); "
          + getId(Consts.RIO_PREFIX + "midx") + ".hasNext();) {\n");
      cb.append(entryType + getId(Consts.RIO_PREFIX + "me") + " = "
          + getId(Consts.RIO_PREFIX + "midx") + ".next();\n");
      cb.append(key.getType() + " " + getId(Consts.RIO_PREFIX + "k") + " = "
          + getId(Consts.RIO_PREFIX + "me") + ".getKey();\n");
      cb.append(value.getType() + " " + getId(Consts.RIO_PREFIX + "v") + " = "
          + getId(Consts.RIO_PREFIX + "me") + ".getValue();\n");
//      key.genWriteMethod(cb, getId(Consts.RIO_PREFIX + "k"),
//          getId(Consts.RIO_PREFIX + "k"));
      key.genRCWriteMethod(cb, getId(Consts.RIO_PREFIX + "k"), indx);
//      value.genWriteMethod(cb, getId(Consts.RIO_PREFIX + "v"),
//          getId(Consts.RIO_PREFIX + "v"));
      value.genRCWriteMethod(cb, getId(Consts.RIO_PREFIX + "v"), indx);
      cb.append("}\n");
//      cb.append(Consts.RECORD_OUTPUT + ".endMap(" + fname + ",\"" + tag
//          + "\");\n");
      cb.append("}\n");
      decrLevel();
    }
//    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
//      cb.append("{\n");
//      incrLevel();
//      cb.append("int "+getId("mi")+
//                " = org.apache.hadoop.record.Utils.readVInt("+b+", "+s+");\n");
//      cb.append("int "+getId("mz")+
//                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("mi")+");\n");
//      cb.append(s+"+="+getId("mz")+"; "+l+"-="+getId("mz")+";\n");
//      cb.append("for (int "+getId("midx")+" = 0; "+getId("midx")+
//                " < "+getId("mi")+"; "+getId("midx")+"++) {");
//      key.genSlurpBytes(cb, b, s, l);
//      value.genSlurpBytes(cb, b, s, l);
//      cb.append("}\n");
//      decrLevel();
//      cb.append("}\n");
//    }
    
//    void genCompareBytes(CodeBuffer cb) {
//      cb.append("{\n");
//      incrLevel();
//      cb.append("int "+getId("mi1")+
//                " = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
//      cb.append("int "+getId("mi2")+
//                " = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
//      cb.append("int "+getId("mz1")+
//                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("mi1")+");\n");
//      cb.append("int "+getId("mz2")+
//                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("mi2")+");\n");
//      cb.append("s1+="+getId("mz1")+"; s2+="+getId("mz2")+
//                "; l1-="+getId("mz1")+"; l2-="+getId("mz2")+";\n");
//      cb.append("for (int "+getId("midx")+" = 0; "+getId("midx")+
//                " < "+getId("mi1")+" && "+getId("midx")+" < "+getId("mi2")+
//                "; "+getId("midx")+"++) {");
//      key.genCompareBytes(cb);
//      value.genSlurpBytes(cb, "b1", "s1", "l1");
//      value.genSlurpBytes(cb, "b2", "s2", "l2");
//      cb.append("}\n");
//      cb.append("if ("+getId("mi1")+" != "+getId("mi2")+
//                ") { return ("+getId("mi1")+"<"+getId("mi2")+")?-1:0; }\n");
//      decrLevel();
//      cb.append("}\n");
//    }
   }
  
    /** Creates a new instance of JRcMap */
    public JRcMap(JRcType t1, JRcType t2) {
      setJavaType(new JavaMap(t1.getJavaType(), t2.getJavaType()));
      keyType = t1;
      valueType = t2;
    }

    String getSignature() {
      return "{" + keyType.getSignature() + valueType.getSignature() + "}";
    }
}
