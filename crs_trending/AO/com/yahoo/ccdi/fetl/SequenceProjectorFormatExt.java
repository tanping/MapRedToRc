package com.yahoo.ccdi.fetl;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.util.*;

import com.yahoo.ccdi.fetl.sequence.mapreduce.SequenceProjector; 
import com.yahoo.ccdi.fetl.sequence.mapreduce.SequenceProjectorFormat; 
import com.yahoo.ccdi.fetl.sequence.mapreduce.Projection;              

public class SequenceProjectorFormatExt extends SequenceProjectorFormat{
  static  SequenceProjector  reader;
  
  @Override
        public RecordReader<Vector<Object>, Vector<Object> > createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    if(reader == null){
      reader = (SequenceProjector)super.createRecordReader(split, context);
    }
    return reader;
  }
  
//  @Override
//  protected long getFormatMinSplitSize() {
//    //return SequenceFile.SYNC_INTERVAL;
//    // make it non-splittable
//    return GlobalConfVarNames.FORMAT_MIN_SPLIT_SIZE;
//  }
  
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // make it non-splittable
    return false;
  }
}