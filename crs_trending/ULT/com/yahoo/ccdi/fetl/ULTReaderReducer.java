/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ULT.com.yahoo.ccdi.fetl;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 * @author ramdurga
 */
public class ULTReaderReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private final static IntWritable SumValue = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}

			SumValue.set(sum);
			context.write(key, SumValue);
		}
	}