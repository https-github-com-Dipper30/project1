package hadoop;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Log1 {

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
				String request = m.group("r");
				String findPath = "(?<method>\"\\S+)\\s(?<path>\\S+)\\s(?<protocol>\\S+\")";
				r = Pattern.compile(findPath);
				m = r.matcher(request);
				if (m.find() && m.group("path").equals("/assets/img/home-logo.png")) {
					word.set(m.group("path"));
	                context.write(word, one);
				}
            }
        }
    }

    public static class LogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
     * How many hits were made to the website item “/assets/img/home-logo.png”
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // reminder of wrong commands
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 2) {
                System.err.println("Usage: Log1 <input-path> <output-path>");
                System.exit(2);
        }
        
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Job job = Job.getInstance(conf, "log1");
        job.setJarByClass(Log1.class);
        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(LogReducer.class);
        job.setReducerClass(LogReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

