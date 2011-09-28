package com.yahoo.ccdi.fetl.compiler;

import java.util.Map;
import org.apache.hadoop.record.compiler.Consts;

/**
 * Due to the limited visibility of some mebers of 
 * org.apache.hadoop.record.compiler.JType, we have to copy particial code of 
 * org.apache.hadoop.record.compiler.JType here.
 */
public abstract class JRcType{

  static String toCamelCase(String name) {
    char firstChar = name.charAt(0);
    if (Character.isLowerCase(firstChar)) {
      return ""+Character.toUpperCase(firstChar) + name.substring(1);
    }
    return name;
  }
  
  JavaType javaType;
  
  abstract class JavaType {
    private String name;
    private String methodSuffix;
    private String wrapper;
    private String typeIDByteString; // points to TypeID.RIOType 
    
    JavaType(String javaname,
        String suffix,
        String wrapper, 
        String typeIDByteString) { 
      this.name = javaname;
      this.methodSuffix = suffix;
      this.wrapper = wrapper;
      this.typeIDByteString = typeIDByteString;
    }

    void genDecl(CodeBuffer cb, String fname) {
      cb.append("private "+name+" "+fname+";\n");
    }
    
    void genStaticTypeInfo(CodeBuffer cb, String fname) {
      cb.append(Consts.RTI_VAR + ".addField(\"" + fname + "\", " +
          getTypeIDObjectString() + ");\n");
    }
    
    abstract String getTypeIDObjectString();
    
    void genSetRTIFilter(CodeBuffer cb, Map<String, Integer> nestedStructMap) {
      // do nothing by default
      return;
    }

    void genConstructorParam(CodeBuffer cb, String fname) {
      cb.append("final "+name+" "+fname);
    }
    
    void genGetSet(CodeBuffer cb, String fname) {
      cb.append("public "+name+" get"+toCamelCase(fname)+"() {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("public void set"+toCamelCase(fname)+"(final "+name+" "+fname+") {\n");
      cb.append("this."+fname+"="+fname+";\n");
      cb.append("}\n");
    }
    
    String getType() {
      return name;
    }
    
    String getWrapperType() {
      return wrapper;
    }
    
    String getMethodSuffix() {
      return methodSuffix;
    }
    
    String getTypeIDByteString() {
      return typeIDByteString;
    }
    
//    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
//      cb.append(Consts.RECORD_OUTPUT + ".write"+methodSuffix + 
//          "("+fname+",\""+tag+"\");\n");
//    }
    
    void genRCWriteMethod(CodeBuffer cb, String fname, String index) {
      cb.append(RcConsts.RCTUIL + ".write"+methodSuffix + 
          "(this, "+ fname+", " + index +"++);\n");
    }
    
//    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
//      if (decl) {
//        cb.append(name+" "+fname+";\n");
//      }
//      cb.append(fname+"=" + Consts.RECORD_INPUT + ".read" + 
//          methodSuffix+"(\""+tag+"\");\n");
//    }
    
    void genRCReadMethod(CodeBuffer cb, String fname, String bra_var_name, 
        String index, boolean decl) {
      if (decl) {
        cb.append(name+" "+fname+";\n");
      }
      cb.append(fname+"=" + RcConsts.RCTUIL + ".read" + 
          methodSuffix+"("+bra_var_name+", "+ index +"++);\n");
    }
    
    void genCompareTo(CodeBuffer cb, String fname, String other) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+" == "+other+")? 0 :(("+
          fname+"<"+other+")?-1:1);\n");
    }
    
    //abstract void genCompareBytes(CodeBuffer cb);
    
    //abstract void genSlurpBytes(CodeBuffer cb, String b, String s, String l);
    
    void genEquals(CodeBuffer cb, String fname, String peer) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+"=="+peer+");\n");
    }
    
    void genHashCode(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = (int)"+fname+";\n");
    }
    
    void genConstructorSet(CodeBuffer cb, String fname) {
      cb.append("this."+fname+" = "+fname+";\n");
    }
    
    void genClone(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "other."+fname+" = this."+fname+";\n");
    }
  }
 
  abstract String getSignature();
  
  void setJavaType(JavaType jType) {
    this.javaType = jType;
  }
  
  JavaType getJavaType() {
    return javaType;
  }
}
