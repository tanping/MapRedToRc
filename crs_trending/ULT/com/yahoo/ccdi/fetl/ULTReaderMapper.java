package ULT.com.yahoo.ccdi.fetl;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.record.Buffer;

public class ULTReaderMapper extends // Mapper<Text, ULTRecordJT, MetricsKeyType, MetricsValueType> {
        Mapper<Vector<Object>, Vector<Object>, Text, IntWritable> {

    HashMap<String, Integer> myMap;
    public static JobContext sJobContext = null;
    Properties props = null;
    String[] propertySplit;
    Configuration jConf = null;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        jConf = context.getConfiguration();
        sJobContext = context;
        String properties = jConf.get("properties");
        System.out.println("Properties :" + properties);
        if (properties != null && !properties.isEmpty()) {
            props = new Properties();
            String[] strArray;
            propertySplit = properties.split(",");
            for (String s : propertySplit) {
                strArray = s.split(":");
                props.put(strArray[0], strArray[1]);
            }
        } else {
            throw new RuntimeException("Missing properties ");
        }



    }

    @Override
    public void map(Vector<Object> key, Vector<Object> value,
            /* Text key, ULTRecordJT value, */ Context context) throws IOException,
            InterruptedException {
        //myMap = new HashMap<String, Integer>();

        if (!props.isEmpty()) {


            int i = 0;
            for (Object obj : value) {

                String[] strArry = propertySplit[i].split(":");
                String propertyType = props.getProperty(strArry[0]);
                //System.out.println("working on " + strArry[0] + "value is:" + props.getProperty(propertyType));
                if (propertyType != null && propertyType.equalsIgnoreCase("Buffer")) {
                    Buffer res = (Buffer) value.get(i);
                    //myMap.put(propertySplit[i], 1);
                    if (res != null && res.getCount() > 0) {
                        context.getCounter("ULT", strArry[0]).increment(1);
                    }
                    context.write(new Text(strArry[0]), new IntWritable(1));
                } else if (propertyType != null && propertyType.equalsIgnoreCase("Map")) {
                    HashMap<Object, Object> map = (HashMap<Object, Object>) value.get(i);
                    if (map != null && !map.isEmpty()) {
                        context.getCounter("ULT", strArry[0]).increment(map.size());
                        context.write(new Text(strArry[0]), new IntWritable(map.size()));
                    }
                } else if (propertyType != null && propertyType.equalsIgnoreCase("Vector")) {
                    Vector<Object> vector = (Vector<Object>) value.get(i);
                    if (vector != null) {
                        for (int resultOffset = 0; resultOffset < vector.size(); resultOffset++) {
                            HashMap<Object, Object> resultMap = (HashMap<Object, Object>) vector.get(resultOffset);
                            //myMap.put(propertySplit[i], resultMap.size());
                            if (resultMap != null && !resultMap.isEmpty()) {
                                context.getCounter("ULT", strArry[0]).increment(resultMap.size());
                                context.write(new Text(strArry[0]), new IntWritable(resultMap.size()));
                            }
                        }
                    }
                } else {
                    //Nothing to do for Now..
                }
                i++;
            }


        }
    }
}
