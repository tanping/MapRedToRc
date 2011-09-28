package com.yahoo.ccdi.fetl.compiler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.record.compiler.Consts;


public class JRcRecord extends JRcCompType{
  
  class JavaRecord extends JavaCompType {

    private String fullName;
    private String name;
    private String module;
    private ArrayList<JRcField<JavaType>> fields =
      new ArrayList<JRcField<JavaType>>();
    
    JavaRecord(String name, ArrayList<JRcField<JRcType>> flist) {
      super(name, "Record", name, "TypeID.RIOType.STRUCT");
      this.fullName = name;
      int idx = name.lastIndexOf('.');
      this.name = name.substring(idx+1);
      this.module = name.substring(0, idx);
      for (Iterator<JRcField<JRcType>> iter = flist.iterator(); iter.hasNext();) {
        JRcField<JRcType> f = iter.next();
        fields.add(new JRcField<JavaType>(f.getName(), f.getType().getJavaType()));
      }
    }
    
    String getTypeIDObjectString() {
      return "new org.apache.hadoop.record.meta.StructTypeID(" + 
      fullName + ".getTypeInfo())";
    }

    void genSetRTIFilter(CodeBuffer cb, Map<String, Integer> nestedStructMap) {
      // ignore, if we'ev already set the type filter for this record
      if (!nestedStructMap.containsKey(fullName)) {
        // we set the RTI filter here
        cb.append(fullName + ".setTypeFilter(rti.getNestedStructTypeInfo(\""+
            name + "\"));\n");
        nestedStructMap.put(fullName, null);
      }
    }

    // for each typeInfo in the filter, we see if there's a similar one in the record. 
    // Since we store typeInfos in ArrayLists, thsi search is O(n squared). We do it faster
    // if we also store a map (of TypeInfo to index), but since setupRtiFields() is called
    // only once when deserializing, we're sticking with the former, as the code is easier.  
    void genSetupRtiFields(CodeBuffer cb) {
      cb.append("private static void setupRtiFields()\n{\n");
      cb.append("if (null == " + Consts.RTI_FILTER + ") return;\n");
      cb.append("// we may already have done this\n");
      cb.append("if (null != " + Consts.RTI_FILTER_FIELDS + ") return;\n");
      cb.append("int " + Consts.RIO_PREFIX + "i, " + Consts.RIO_PREFIX + "j;\n");
      cb.append(Consts.RTI_FILTER_FIELDS + " = new int [" + 
          Consts.RIO_PREFIX + "rtiFilter.getFieldTypeInfos().size()];\n");
      cb.append("for (" + Consts.RIO_PREFIX + "i=0; " + Consts.RIO_PREFIX + "i<"+
          Consts.RTI_FILTER_FIELDS + ".length; " + Consts.RIO_PREFIX + "i++) {\n");
      cb.append(Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + "i] = 0;\n");
      cb.append("}\n");
      cb.append("java.util.Iterator<org.apache.hadoop.record.meta." +
          "FieldTypeInfo> " + Consts.RIO_PREFIX + "itFilter = " + 
          Consts.RIO_PREFIX + "rtiFilter.getFieldTypeInfos().iterator();\n");
      cb.append(Consts.RIO_PREFIX + "i=0;\n");
      cb.append("while (" + Consts.RIO_PREFIX + "itFilter.hasNext()) {\n");
      cb.append("org.apache.hadoop.record.meta.FieldTypeInfo " + 
          Consts.RIO_PREFIX + "tInfoFilter = " + 
          Consts.RIO_PREFIX + "itFilter.next();\n");
      cb.append("java.util.Iterator<org.apache.hadoop.record.meta." + 
          "FieldTypeInfo> " + Consts.RIO_PREFIX + "it = " + Consts.RTI_VAR + 
          ".getFieldTypeInfos().iterator();\n");
      cb.append(Consts.RIO_PREFIX + "j=1;\n");
      cb.append("while (" + Consts.RIO_PREFIX + "it.hasNext()) {\n");
      cb.append("org.apache.hadoop.record.meta.FieldTypeInfo " + 
          Consts.RIO_PREFIX + "tInfo = " + Consts.RIO_PREFIX + "it.next();\n");
      cb.append("if (" + Consts.RIO_PREFIX + "tInfo.equals(" +  
          Consts.RIO_PREFIX + "tInfoFilter)) {\n");
      cb.append(Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + "i] = " +
          Consts.RIO_PREFIX + "j;\n");
      cb.append("break;\n");
      cb.append("}\n");
      cb.append(Consts.RIO_PREFIX + "j++;\n");
      cb.append("}\n");
      cb.append(Consts.RIO_PREFIX + "i++;\n");
      cb.append("}\n");
      cb.append("}\n");
    }

//    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
//      if (decl) {
//        cb.append(fullName+" "+fname+";\n");
//      }
//      cb.append(fname+"= new "+fullName+"();\n");
//      cb.append(fname+".deserialize(" + Consts.RECORD_INPUT + ",\""+tag+"\");\n");
//    }
    
    void genRCReadMethod(CodeBuffer cb, String fname,  boolean decl) {
      if (decl) {
        cb.append(fullName+" "+fname+";\n");
      }
      cb.append(fname+"= new "+fullName+"();\n");
      //cb.append(fname+".deserialize(" + Consts.RECORD_INPUT + ",\""+tag+"\");\n");
      cb.append(fname+".deserialize(" 
          + "org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable "
          + RcConsts.BRA_VAR 
          +"\");\n");
    }
    
//    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
//      cb.append(fname+".serialize(" + Consts.RECORD_OUTPUT + ",\""+tag+"\");\n");
//    }
    
    void genRCWriteMethod(CodeBuffer cb, String fname) {
      cb.append(fname+".serialize();\n");
    }
    
    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
      cb.append("{\n");
      cb.append("int r = "+fullName+
                ".Comparator.slurpRaw("+b+","+s+","+l+");\n");
      cb.append(s+"+=r; "+l+"-=r;\n");
      cb.append("}\n");
    }
    
    void genCompareBytes(CodeBuffer cb) {
      cb.append("{\n");
      cb.append("int r1 = "+fullName+
                ".Comparator.compareRaw(b1,s1,l1,b2,s2,l2);\n");
      cb.append("if (r1 <= 0) { return r1; }\n");
      cb.append("s1+=r1; s2+=r1; l1-=r1; l2-=r1;\n");
      cb.append("}\n");
    }
    
    void genCode(String destDir, ArrayList<String> options) throws IOException {
      String pkg = module;
      String pkgpath = pkg.replaceAll("\\.", "/");
      File pkgdir = new File(destDir, pkgpath);

      final File jfile = new File(pkgdir, name+".java");
      if (!pkgdir.exists()) {
        // create the pkg directory
        boolean ret = pkgdir.mkdirs();
        if (!ret) {
          throw new IOException("Cannnot create directory: "+pkgpath);
        }
      } else if (!pkgdir.isDirectory()) {
        // not a directory
        throw new IOException(pkgpath+" is not a directory.");
      }

      CodeBuffer cb = new CodeBuffer();
      cb.append("// File generated by hadoop record compiler. Do not edit.\n");
      cb.append("package "+module+";\n\n");
      cb.append("public class "
          + name
          + " extends org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable {\n");
      
      // type information declarations
      cb.append("private static final " + 
          "org.apache.hadoop.record.meta.RecordTypeInfo " + 
          Consts.RTI_VAR + ";\n");
      cb.append("private static " + 
          "org.apache.hadoop.record.meta.RecordTypeInfo " + 
          Consts.RTI_FILTER + ";\n");
      cb.append("private static int[] " + Consts.RTI_FILTER_FIELDS + ";\n");
      cb.append("private static int " + RcConsts.WRITE_INDEX + " = 0;\n");
      
      // static init for type information
      cb.append("static {\n");
      cb.append(Consts.RTI_VAR + " = " +
          "new org.apache.hadoop.record.meta.RecordTypeInfo(\"" +
          name + "\");\n");
      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JRcField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genStaticTypeInfo(cb, name);
      }
      cb.append("}\n\n");

      // field definitions
      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JRcField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genDecl(cb, name);
      }

      // default constructor
      cb.append("public "+name+"() { }\n");
      
      // constructor
      cb.append("public "+name+"(\n");
      int fIdx = 0;
      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext(); fIdx++) {
        JRcField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genConstructorParam(cb, name);
        cb.append((!i.hasNext())?"":",\n");
      }
      cb.append(") {\n");
      fIdx = 0;
      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext(); fIdx++) {
        JRcField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genConstructorSet(cb, name);
      }
      cb.append("this.serialize();\n");
      cb.append("}\n");

      // getter/setter for type info
      cb.append("public static org.apache.hadoop.record.meta.RecordTypeInfo"
              + " getTypeInfo() {\n");
      cb.append("return " + Consts.RTI_VAR + ";\n");
      cb.append("}\n");
      cb.append("public static void setTypeFilter("
          + "org.apache.hadoop.record.meta.RecordTypeInfo rti) {\n");
      cb.append("if (null == rti) return;\n");
      cb.append(Consts.RTI_FILTER + " = rti;\n");
      cb.append(Consts.RTI_FILTER_FIELDS + " = null;\n");
      // set RTIFilter for nested structs.
      // To prevent setting up the type filter for the same struct more than once, 
      // we use a hash map to keep track of what we've set. 
      Map<String, Integer> nestedStructMap = new HashMap<String, Integer>();
      for (JRcField<JavaType> jf : fields) {
        JavaType type = jf.getType();
        type.genSetRTIFilter(cb, nestedStructMap);
      }
      cb.append("}\n");

      // setupRtiFields()
      genSetupRtiFields(cb);

      // getters/setters for member variables
      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JRcField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genGetSet(cb, name);
      }
      
      // serialize()
      cb.append("public void serialize("+ 
//          "final org.apache.hadoop.record.RecordOutput " + 
//          Consts.RECORD_OUTPUT + ", final String " + Consts.TAG + 
          ") {\n"  );
//          +"throws java.io.IOException {\n");
      
//      cb.append(Consts.RECORD_OUTPUT + ".startRecord(this," + Consts.TAG + ");\n");
      cb.append("int " + RcConsts.WRITE_INDEX + " = 0;\n");
      cb.append("try {\n");
      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JRcField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genRCWriteMethod(cb, name, RcConsts.WRITE_INDEX);
      }
      cb.append("} catch(java.io.IOException e) {\n");
      cb.append("e.printStackTrace();\n");
//      cb.append(Consts.RECORD_OUTPUT + ".endRecord(this," + Consts.TAG+");\n");
      cb.append("}\n");
      cb.append("}\n");

      // deserializeWithoutFilter()
      cb.append("public void deserialize("
//                + "final org.apache.hadoop.record.RecordInput " + 
//                Consts.RECORD_INPUT + ", final String " + Consts.TAG + ")\n"+
//                "throws java.io.IOException {\n"
          + "org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable "
          + RcConsts.BRA_VAR 
          + "){\n"
          );
//      cb.append(Consts.RECORD_INPUT + ".startRecord(" + Consts.TAG + ");\n");
      cb.append("int "+ RcConsts.READ_INDEX + " = 0;\n");
      cb.append("try {\n");
      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JRcField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genRCReadMethod(cb, name, RcConsts.BRA_VAR ,RcConsts.READ_INDEX, false);
      }
      cb.append("} catch(java.io.IOException e) {\n");
      cb.append("e.printStackTrace();\n");
//      cb.append(Consts.RECORD_INPUT + ".endRecord(" + Consts.TAG+");\n");
      cb.append("}\n");
      cb.append("}\n");

      
//      // deserialize()
//      cb.append("public void deserialize(final " +
//          "org.apache.hadoop.record.RecordInput " + 
//          Consts.RECORD_INPUT + ", final String " + Consts.TAG + ")\n"+
//          "throws java.io.java.io.IOException {\n");
//      cb.append("if (null == " + Consts.RTI_FILTER + ") {\n");
//      cb.append("deserializeWithoutFilter(" + Consts.RECORD_INPUT + ", " + 
//          Consts.TAG + ");\n");
//      cb.append("return;\n");
//      cb.append("}\n");
//      cb.append("// if we're here, we need to read based on version info\n");
//      cb.append(Consts.RECORD_INPUT + ".startRecord(" + Consts.TAG + ");\n");
//      cb.append("setupRtiFields();\n");
//      cb.append("for (int " + Consts.RIO_PREFIX + "i=0; " + Consts.RIO_PREFIX + 
//          "i<" + Consts.RTI_FILTER + ".getFieldTypeInfos().size(); " + 
//          Consts.RIO_PREFIX + "i++) {\n");
//      int ct = 0;
//      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
//        JRcField<JavaType> jf = i.next();
//        String name = jf.getName();
//        JavaType type = jf.getType();
//        ct++;
//        if (1 != ct) {
//          cb.append("else ");
//        }
//        cb.append("if (" + ct + " == " + Consts.RTI_FILTER_FIELDS + "[" +
//            Consts.RIO_PREFIX + "i]) {\n");
//        type.genReadMethod(cb, name, name, false);
//        cb.append("}\n");
//      }
//      if (0 != ct) {
//        cb.append("else {\n");
//        cb.append("java.util.ArrayList<"
//                + "org.apache.hadoop.record.meta.FieldTypeInfo> typeInfos = "
//                + "(java.util.ArrayList<"
//                + "org.apache.hadoop.record.meta.FieldTypeInfo>)"
//                + "(" + Consts.RTI_FILTER + ".getFieldTypeInfos());\n");
//        cb.append("org.apache.hadoop.record.meta.Utils.skip(" + 
//            Consts.RECORD_INPUT + ", " + "typeInfos.get(" + Consts.RIO_PREFIX + 
//            "i).getFieldID(), typeInfos.get(" + 
//            Consts.RIO_PREFIX + "i).getTypeID());\n");
//        cb.append("}\n");
//      }
//      cb.append("}\n");
//      cb.append(Consts.RECORD_INPUT + ".endRecord(" + Consts.TAG+");\n");
//      cb.append("}\n");

//      // compareTo()
//      cb.append("public int compareTo (final Object " + Consts.RIO_PREFIX + 
//          "peer_) throws ClassCastException {\n");
//      cb.append("if (!(" + Consts.RIO_PREFIX + "peer_ instanceof "+name+")) {\n");
//      cb.append("throw new ClassCastException(\"Comparing different types of records.\");\n");
//      cb.append("}\n");
//      cb.append(name+" " + Consts.RIO_PREFIX + "peer = ("+name+") " + 
//          Consts.RIO_PREFIX + "peer_;\n");
//      cb.append("int " + Consts.RIO_PREFIX + "ret = 0;\n");
//      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
//        JRcField<JavaType> jf = i.next();
//        String name = jf.getName();
//        JavaType type = jf.getType();
//        type.genCompareTo(cb, name, Consts.RIO_PREFIX + "peer."+name);
//        cb.append("if (" + Consts.RIO_PREFIX + "ret != 0) return " + 
//            Consts.RIO_PREFIX + "ret;\n");
//      }
//      cb.append("return " + Consts.RIO_PREFIX + "ret;\n");
//      cb.append("}\n");
      
//      // equals()
//      cb.append("public boolean equals(final Object " + Consts.RIO_PREFIX + 
//          "peer_) {\n");
//      cb.append("if (!(" + Consts.RIO_PREFIX + "peer_ instanceof "+name+")) {\n");
//      cb.append("return false;\n");
//      cb.append("}\n");
//      cb.append("if (" + Consts.RIO_PREFIX + "peer_ == this) {\n");
//      cb.append("return true;\n");
//      cb.append("}\n");
//      cb.append(name+" " + Consts.RIO_PREFIX + "peer = ("+name+") " + 
//          Consts.RIO_PREFIX + "peer_;\n");
//      cb.append("boolean " + Consts.RIO_PREFIX + "ret = false;\n");
//      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
//        JRcField<JavaType> jf = i.next();
//        String name = jf.getName();
//        JavaType type = jf.getType();
//        type.genEquals(cb, name, Consts.RIO_PREFIX + "peer."+name);
//        cb.append("if (!" + Consts.RIO_PREFIX + "ret) return " + 
//            Consts.RIO_PREFIX + "ret;\n");
//      }
//      cb.append("return " + Consts.RIO_PREFIX + "ret;\n");
//      cb.append("}\n");

//      // clone()
//      cb.append("public Object clone() throws CloneNotSupportedException {\n");
//      cb.append(name+" " + Consts.RIO_PREFIX + "other = new "+name+"();\n");
//      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
//        JRcField<JavaType> jf = i.next();
//        String name = jf.getName();
//        JavaType type = jf.getType();
//        type.genClone(cb, name);
//      }
//      cb.append("return " + Consts.RIO_PREFIX + "other;\n");
//      cb.append("}\n");
      
//      // hashCode
//      cb.append("public int hashCode() {\n");
//      cb.append("int " + Consts.RIO_PREFIX + "result = 17;\n");
//      cb.append("int " + Consts.RIO_PREFIX + "ret;\n");
//      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
//        JRcField<JavaType> jf = i.next();
//        String name = jf.getName();
//        JavaType type = jf.getType();
//        type.genHashCode(cb, name);
//        cb.append(Consts.RIO_PREFIX + "result = 37*" + Consts.RIO_PREFIX + 
//            "result + " + Consts.RIO_PREFIX + "ret;\n");
//      }
//      cb.append("return " + Consts.RIO_PREFIX + "result;\n");
//      cb.append("}\n");
//      
//      // signature
//      cb.append("public static String signature() {\n");
//      cb.append("return \""+getSignature()+"\";\n");
//      cb.append("}\n");
      
//      // Comparator
//      cb.append("public static class Comparator extends"+
//                " org.apache.hadoop.record.RecordComparator {\n");
//      cb.append("public Comparator() {\n");
//      cb.append("super("+name+".class);\n");
//      cb.append("}\n");
//      
//        // slurpRaw
//      cb.append("static public int slurpRaw(byte[] b, int s, int l) {\n");
//      cb.append("try {\n");
//      cb.append("int os = s;\n");
//      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
//        JRcField<JavaType> jf = i.next();
//        String name = jf.getName();
//        JavaType type = jf.getType();
//        type.genSlurpBytes(cb, "b","s","l");
//      }
//      cb.append("return (os - s);\n");
//      cb.append("} catch(java.io.IOException e) {\n");
//      cb.append("throw new RuntimeException(e);\n");
//      cb.append("}\n");
//      cb.append("}\n");
//      
//      // compareRaw
//      cb.append("static public int compareRaw(byte[] b1, int s1, int l1,\n");
//      cb.append("                             byte[] b2, int s2, int l2) {\n");
//      cb.append("try {\n");
//      cb.append("int os1 = s1;\n");
//      for (Iterator<JRcField<JavaType>> i = fields.iterator(); i.hasNext();) {
//        JRcField<JavaType> jf = i.next();
//        String name = jf.getName();
//        JavaType type = jf.getType();
//        type.genCompareBytes(cb);
//      }
//      cb.append("return (os1 - s1);\n");
//      cb.append("} catch(java.io.IOException e) {\n");
//      cb.append("throw new RuntimeException(e);\n");
//      cb.append("}\n");
//      cb.append("}\n");
//      
//      // compare
//      cb.append("public int compare(byte[] b1, int s1, int l1,\n");
//      cb.append("                   byte[] b2, int s2, int l2) {\n");
//      cb.append("int ret = compareRaw(b1,s1,l1,b2,s2,l2);\n");
//      cb.append("return (ret == -1)? -1 : ((ret==0)? 1 : 0);");
//      cb.append("}\n");
//      cb.append("}\n\n");
//      
//      // static
//      cb.append("static {\n");
//      cb.append("org.apache.hadoop.record.RecordComparator.define("
//                +name+".class, new Comparator());\n");
//      cb.append("}\n");
      
      cb.append("}\n");

      // generate the code
      FileWriter jj = new FileWriter(jfile);
      try {
        jj.write(cb.toString());
      } finally {
        jj.close();
      }
    }
  
    
  }
  
  
  private String signature;
  
  /**
   * Creates a new instance of JRecord
   */
  public JRcRecord(String name, ArrayList<JRcField<JRcType>> flist) {
    setJavaType(new JavaRecord(name, flist));
    // precompute signature
    int idx = name.lastIndexOf('.');
    String recName = name.substring(idx+1);
    StringBuffer sb = new StringBuffer();
    sb.append("L").append(recName).append("(");
    for (Iterator<JRcField<JRcType>> i = flist.iterator(); i.hasNext();) {
      String s = i.next().getType().getSignature();
      sb.append(s);
    }
    sb.append(")");
    signature = sb.toString();
  }
  
  String getSignature() {
    return signature;
  }
  
//  void genCppCode(FileWriter hh, FileWriter cc, ArrayList<String> options)
//    throws IOException {
//    ((CppRecord)getCppType()).genCode(hh, cc, options);
//  }
//  
//  void genJavaCode(String destDir, ArrayList<String> options)
//    throws IOException {
//    ((JavaRecord)getJavaType()).genCode(destDir, options);
//  }

  public void genJavaRcCode(String destDir, ArrayList<String> options)    
    throws IOException {
    ((JavaRecord)getJavaType()).genCode(destDir, options);
  }
}
