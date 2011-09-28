package com.yahoo.ccdi.fetl.compiler;

/**
 * Due to private member of CodeBuffer is visiable to its subclasses.  
 * We copied the code of org.apache.hadoop.record.compiler.CodeBuffer
 * completely.
 */

import java.util.ArrayList;

/**
 * A wrapper around StringBuffer that automatically does indentation
 */
public class CodeBuffer {
  
  static private ArrayList<Character> startMarkers = new ArrayList<Character>();
  static private ArrayList<Character> endMarkers = new ArrayList<Character>();
  
  static {
    addMarkers('{', '}');
    addMarkers('(', ')');
  }
  
  static void addMarkers(char ch1, char ch2) {
    startMarkers.add(ch1);
    endMarkers.add(ch2);
  }
  
  private int level = 0;
  private int numSpaces = 2;
  private boolean firstChar = true;
  private StringBuffer sb;
  
  /** Creates a new instance of CodeBuffer */
  CodeBuffer() {
    this(2, "");
  }
  
  CodeBuffer(String s) {
    this(2, s);
  }
  
  CodeBuffer(int numSpaces, String s) {
    sb = new StringBuffer();
    this.numSpaces = numSpaces;
    this.append(s);
  }
  
  void append(String s) {
    int length = s.length();
    for (int idx = 0; idx < length; idx++) {
      char ch = s.charAt(idx);
      append(ch);
    }
  }
  
  void append(char ch) {
    if (endMarkers.contains(ch)) {
      level--;
    }
    if (firstChar) {
      for (int idx = 0; idx < level; idx++) {
        for (int num = 0; num < numSpaces; num++) {
          rawAppend(' ');
        }
      }
    }
    rawAppend(ch);
    firstChar = false;
    if (startMarkers.contains(ch)) {
      level++;
    }
    if (ch == '\n') {
      firstChar = true;
    }
  }

  private void rawAppend(char ch) {
    sb.append(ch);
  }
  
  public String toString() {
    return sb.toString();
  }
}
