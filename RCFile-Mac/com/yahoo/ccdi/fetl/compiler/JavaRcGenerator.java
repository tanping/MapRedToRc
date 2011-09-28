package com.yahoo.ccdi.fetl.compiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.record.compiler.JFile;
import org.apache.hadoop.record.compiler.JRecord;

public class JavaRcGenerator extends CodeGenerator{
  
  JavaRcGenerator() {
  }
  
  @Override
  void genCode(String name, ArrayList<JRcFile> ilist,
      ArrayList<JRcRecord> rlist, String destDir, ArrayList<String> options)
      throws IOException {
    for (Iterator<JRcRecord> iter = rlist.iterator(); iter.hasNext();) {
      JRcRecord rec = iter.next();
      rec.genJavaRcCode(destDir, options);
    }
  }

}
