package hadoop;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Log3 {
	
	public static class sortComparator extends WritableComparator {

        protected sortComparator() {
         super(IntWritable.class, true);
         // TODO Auto-generated constructor stub
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
        	 IntWritable k1 = (IntWritable) o1;
        	 IntWritable k2 = (IntWritable) o2;
	         int cmp = k1.compareTo(k2);
	         return -1 * cmp;
        }
	}

	/**
	 * First Mapper
	 */
    public static class LogMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            String re = "^(?<h>\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3})\\s(?<l>\\S+)\\s(?<u>\\S+)\\s(?<t>\\S+\\s\\S+)\\s(?<r>\\S+\\s\\S+\\s\\S+)\\s(?<s>\\S+)\\s(?<b>\\S+)$";
            Pattern r = Pattern.compile(re);
            Matcher m = r.matcher(log);
            if (m.find()) {
				// match and find request, eg: "GET /assets/js/lightbox.js HTTP/1.1"
				String[] request = m.group("r").split(" ");
				if (request.length > 1) {
					String path = request[1];
					word.set(path);
	                context.write(word, one);
				}

            }
        }
    }

    /**
	 * First Reducer
	 * calculate total hits to each website
	 */
    public static class HitSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    /**
	 * Second Mapper
	 */
    public static class WebsiteMapper extends Mapper<Object, Text, IntWritable, Text> {

    	// simply do nothing but deliver the output from the first Reducer to the next Reducer

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
        	String s = value.toString();
        	String re = "(?<path>\\S+)(\\s+)(?<hits>\\S+)";
        	Pattern r = Pattern.compile(re);
            Matcher m = r.matcher(s);
            if (m.find()) {
            	String path = m.group("path");
            	String hits = m.group("hits");
            	
            	// only larger hits can be written to the map result
            	if (Integer.parseInt(hits) >= Integer.parseInt(conf.get("max"))) {
            		conf.set("max", hits);
                	context.write(new IntWritable(Integer.parseInt(hits)), new Text(path));
            	}
            }
        }
    }
    
    /**
	 * Second Reducer
	 * find website(s) with most hits
	 */
    public static class WebsiteReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text web : values) {
            	context.write(key, web);
            }
        }
    }

    /**
     * How many hits were made from the IP: 10.153.239.5
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // reminder of wrong commands
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 3) {
                System.err.println("Usage: Log3 <input-path> <intermediate-output-path> <final-output-path>");
                System.exit(2);
        }
        
        Path inputPath = new Path(args[0]);
        Path outputPath1 = new Path(args[1]);
        Path outputPath2 = new Path(args[2]);
        
        Job job1 = Job.getInstance(conf, "log3: job1");
        job1.setJarByClass(Log3.class);
        job1.setMapperClass(LogMapper.class);
        job1.setCombinerClass(HitSumReducer.class);
        job1.setReducerClass(HitSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath1);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        // set inital count of hit
        conf.set("max", "-1");
        
        Job job2 = Job.getInstance(conf, "log3: job2");
        job2.setJarByClass(Log3.class);
        job2.setMapperClass(WebsiteMapper.class);
        job2.setCombinerClass(WebsiteReducer.class);
        job2.setReducerClass(WebsiteReducer.class);
        job2.setSortComparatorClass(sortComparator.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, outputPath1);
        FileOutputFormat.setOutputPath(job2, outputPath2);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        // multi jobs
        job1.waitForCompletion(true);
        
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

