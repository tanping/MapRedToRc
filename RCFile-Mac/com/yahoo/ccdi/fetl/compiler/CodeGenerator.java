package com.yahoo.ccdi.fetl.compiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * CodeGenerator is a Factory and a base class for Hadoop Record I/O translators.
 * Different translators register creation methods with this factory.
 */
abstract class CodeGenerator {
  
  private static HashMap<String, CodeGenerator> generators =
    new HashMap<String, CodeGenerator>();
  
  static {
    register("javarc", new JavaRcGenerator());
  }
  
  static void register(String lang, CodeGenerator gen) {
    generators.put(lang, gen);
  }
  
  static CodeGenerator get(String lang) {
    return generators.get(lang);
  }
  
  abstract void genCode(String file,
                        ArrayList<JRcFile> inclFiles,
                        ArrayList<JRcRecord> records,
                        String destDir,
                        ArrayList<String> options) throws IOException;
}
