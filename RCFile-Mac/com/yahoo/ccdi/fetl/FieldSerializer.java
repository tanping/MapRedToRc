/**
 * 
 */
package com.yahoo.ccdi.fetl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.record.Buffer;

/**
 * @author ruish
 *
 */
public class FieldSerializer {
    public static String mapToString(TreeMap<String, Buffer> map) throws IOException {
        Buffer output = new Buffer(); 
        Iterator itr = map.entrySet().iterator();
        boolean first = true;
        while (itr.hasNext()) {
            if (first) {
                first = false;
            } else {
                output.append(", ".getBytes());
            }
            Map.Entry<String, Buffer> entry = (Map.Entry<String, Buffer>)itr.next();
            output.append(entry.getKey().getBytes());
            output.append(" = ".getBytes());
            output.append(entry.getValue().get());
        }

        return output.toString("UTF-8");
    }
    
    public static String serializeMap(TreeMap<String, Buffer> map) throws IOException {
        return '{' + mapToString(map) + '}';
    }
    
    public static String serializeMapAwacs(TreeMap<String, Buffer> map) throws IOException {
        Buffer output = new Buffer(); 
        Iterator itr = map.entrySet().iterator();
        boolean first = true;
        while (itr.hasNext()) {
            if (first) {
                first = false;
            } else {
                output.append("".getBytes());
            }

            Map.Entry<String, Buffer> entry = (Map.Entry<String, Buffer>)itr.next();
            output.append(entry.getKey().getBytes());
            output.append("".getBytes());
            output.append(entry.getValue().get());
        }
        return output.toString("UTF-8");
    }

    public static String mapOfMapToString(TreeMap<String, TreeMap<String, Buffer>> mapFields) throws IOException {
        String output = ""; 
        Iterator itr = mapFields.entrySet().iterator();
        boolean first = true;
        while (itr.hasNext()) {
            if (first) {
                first = false;
            } else {
                output += ", ";
            }
            Map.Entry<String, TreeMap<String, Buffer>> entry = (Map.Entry<String, TreeMap<String, Buffer>>)itr.next();
            output += entry.getKey() + " = " + serializeMap(entry.getValue());
        }
        
        return output;
    }

    public static String serializeListMap(ArrayList<TreeMap<String, Buffer>> listMap) throws IOException {
        String output = "[";
        for (int i = 0; i < listMap.size(); i++) {
            if (i != 0) output += ", ";
            output += serializeMap(listMap.get(i));
        }
        output += ']';
        return output;
    }
    
    public static String serializeListMapAwacs(ArrayList<TreeMap<String, Buffer>> listMap) throws IOException {
        String output = "";
        for (int i = 0; i < listMap.size(); i++) {
            if (i != 0) output += '';
            output += serializeMapAwacs(listMap.get(i));
        }
                
        return output;
    }

    public static String listMapToString(TreeMap<String, ArrayList<TreeMap<String, Buffer>>> listMapFields) throws IOException {
        String output = "";
        Iterator itr = listMapFields.entrySet().iterator();
        boolean first = true;
        while (itr.hasNext()) {
            if (first) {
                first = false;
            } else {
                output += ", ";
            }
            Map.Entry<String, ArrayList<TreeMap<String, Buffer>>> entry = (Map.Entry<String, ArrayList<TreeMap<String, Buffer>>>)itr.next();
            output += entry.getKey() + " = " + serializeListMap(entry.getValue());
        }
        return output;
    }
}
