package com.yahoo.ccdi.fetl.compiler;

/**
 * A thin wrappper around record field.
 */
public class JRcField<T> {
  
  private String name;
  private T type;
  
  /**
   * Creates a new instance of JField
   */
  public JRcField(String name, T type) {
    this.type = type;
    this.name = name;
  }
  
  String getName() {
    return name;
  }
  
  T getType() {
    return type;
  }
}
