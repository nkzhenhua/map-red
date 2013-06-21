import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import java.lang.StringBuffer;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

public class MRjob
{
	public static Path[] localFiles;
 	public static HashMap<Long,Integer> hm = new HashMap<Long,Integer> (); 

	public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
 		public void reduce(Text key, Iterator<Text> value,OutputCollector<Text,Text> output,Reporter reporter) throws IOException {
        		long sum = 0;
  		      	while (value.hasNext()) {
                		String x = value.next().toString();
                		sum = sum + Long.parseLong(x);
        		}
        		output.collect(key, new Text(""+sum));
 		}
	}

 
  	public static class MapClass extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, Text> {

		HashMap<String, String> mLkData = new  HashMap<String, String>();

    		public void map(WritableComparable key, Writable value, 
        		OutputCollector<Text, Text> output, 
        		Reporter reporter) throws IOException {
		
			MulMap arecord = (MulMap)value;
			if( arecord == null )
			{
				return;
			}
            String s2dstr="";
            Buffer s2dbuff=arecord.getSimpleFields().get("s2d");
            if( null != s2dbuff)
            {
                s2dstr = s2dbuff.toString("UTF-8");
                if( s2dstr == null || s2dstr.equals(""))
                {
                    s2dstr="null";
                }
            }else
            {
                s2dstr="null";
            }

			ArrayList<TreeMap<String, Buffer>> arrList = arecord.getMapListFields().get("features");
			}

			return;
   		}
    
   		public void configure(JobConf job) {
		}
  	}

 
  	public static void main(String[] args) throws IOException {
	
    		if( args.length < 5 ){
      			System.err.println("Arguments required - ListOfInputFilesSeparatedBySpace OutputDirectory QueueName JobName NumReducers");
	      		System.exit(1);
    		}

	    	JobConf conf = new JobConf(SequentialToText.class);
    		conf.setInputFormat(SequenceFileInputFormat.class);
	    	conf.setOutputFormat(TextOutputFormat.class);
    		conf.setOutputKeyClass(Text.class);
	    	conf.setOutputValueClass(Text.class);

		conf.setCombinerClass(ReduceClass.class);
	        conf.setReducerClass(ReduceClass.class);
    		conf.setMapperClass(MapClass.class);       
		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
		conf.set("mapred.output.compression.type", "BLOCK");

    		conf.setNumReduceTasks(1);
		for(int i = 0; i < args.length - 4; i++)  {
			FileInputFormat.addInputPath(conf, new Path((String)args[i]));
			System.err.println("input path set as " + args[i]);
		}
		String outputDir = args[args.length - 4];
		System.err.println("output dir is " + outputDir);
		FileOutputFormat.setOutputPath(conf, new Path(outputDir));
		System.err.println("output dir set");

		String queueName = args[args.length - 3];
		System.err.println("Queue name: " + queueName);
		conf.set("mapred.job.queue.name", queueName);

		String jobName = args[args.length - 2];
		conf.setJobName(jobName);

		String numReducers = args[args.length - 1];
		conf.setNumReduceTasks(Integer.parseInt(numReducers));

	    	JobClient.runJob(conf);
    
  	}
}
