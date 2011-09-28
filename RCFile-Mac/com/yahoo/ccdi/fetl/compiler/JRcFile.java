package com.yahoo.ccdi.fetl.compiler;

import java.io.IOException;
import java.util.ArrayList;

public class JRcFile {
  /** Possibly full name of the file */
  private String mName;
  /** Ordered list of included files */
  private ArrayList<JRcFile> mInclFiles;
  /** Ordered list of records declared in this file */
  private ArrayList<JRcRecord> mRecords;
    
  /** Creates a new instance of JFile
   *
   * @param name possibly full pathname to the file
   * @param inclFiles included files (as JFile)
   * @param recList List of records defined within this file
   */
  public JRcFile(String name, ArrayList<JRcFile> inclFiles,
               ArrayList<JRcRecord> recList) {
    mName = name;
    mInclFiles = inclFiles;
    mRecords = recList;
  }
    
  /** Strip the other pathname components and return the basename */
  String getName() {
    int idx = mName.lastIndexOf('/');
    return (idx > 0) ? mName.substring(idx) : mName; 
  }
    
  /** Generate record code in given language. Language should be all
   *  lowercase.
   */
  public int genCode(String language, String destDir, ArrayList<String> options)
    throws IOException {
    CodeGenerator gen = CodeGenerator.get(language);
    if (gen != null) {
      gen.genCode(mName, mInclFiles, mRecords, destDir, options);
    } else {
      System.err.println("Cannnot recognize language:"+language);
      return 1;
    }
    return 0;
  }


}
