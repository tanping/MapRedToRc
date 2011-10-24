package ULT.com.yahoo.ccdi.fetl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yahoo.ccdi.fetl.sequence.mapreduce.SequenceProjectorFormat;
import com.yahoo.yst.sds.ULT.ULTRecordJT;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class ULTReaderDriver extends Configured implements Tool {

    private static JobClient client;
    private static FileSystem hdfs;
    FSDataOutputStream outputStream;

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapred.job.queue.name", "audience_fetl");
        String propertyIds = GlobalConfVarNames.MAIL_PROPERTY_ID + ","
                + GlobalConfVarNames.FRONT_PAGE_PROPERTY_ID + ","
                + GlobalConfVarNames.NEWS_PROPERTY_ID + ","
                + GlobalConfVarNames.FINANCE_PROPERTY_ID + ","
                + GlobalConfVarNames.SEARCH_PROPERTY_ID + ","
                + GlobalConfVarNames.SPORTS_PROPERTY_ID + ","
                + GlobalConfVarNames.OMG_PROPERTY_ID;

        conf.set(GlobalConfVarNames.INTERESTED_PROPERTY_IDS, propertyIds);

        Job job = new Job(conf);
        job.setJarByClass(ULTReaderDriver.class);
        job.setJobName(conf.get("mapred.job.name", "fetl_1.x_ultreader"));

        //SequenceProjectorFormat.setProjections(job,"","bcookie:Buffer, page_params:Map, viewinfo,clickinfo:Map");
        SequenceProjectorFormat.setProjections(job,"",conf.get("properties"));
        // job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setInputFormatClass(SequenceProjectorFormat.class);
        //job.setOutputFormatClass(MetricsTypeTextOutputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(ULTReaderMapper.class);
        job.setReducerClass(ULTReaderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String input = conf.get("input");
        if (input == null || input.isEmpty()) {
            String err = "Please provide input folder using syntax -Dinput=.. ";
            throw new RuntimeException(err);
        }
        FileInputFormat.setInputPaths(job, new Path(input));

        String output = conf.get("output");
        if (output == null || output.isEmpty()) {
            String err = "Please provide output folder using syntax -Doutput=.. ";
            System.err.println(err);
            throw new RuntimeException(err);
        }

        FileOutputFormat.setOutputPath(job, new Path(output));

        // should be on ONE reducer
        job.setNumReduceTasks(1);

        hdfs = FileSystem.get(conf);


        boolean success = job.waitForCompletion(true);
        FSDataOutputStream outputStream = null;
        if (job.isSuccessful()) {
            System.out.println("before creating the file");
            outputStream = hdfs.create(new Path(output + "/result.properties"), true);
            System.out.println("output is :"+output+"/result.properties");
            outputStream.writeUTF("Page-param=" + job.getCounters().findCounter("ULT", "page-param").getValue());
            outputStream.writeUTF("view-info=" + job.getCounters().findCounter("ULT", "view-info").getValue());
            outputStream.writeUTF("click-info=" + job.getCounters().findCounter("ULT", "click-info").getValue());
            outputStream.flush();outputStream.close();
        } else {
            System.out.println("Job is a failure");
        }

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int ret = ToolRunner.run(conf, new ULTReaderDriver(), otherArgs);

    }
}
