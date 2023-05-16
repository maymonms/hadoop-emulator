package uk.ac.reading.compsci.csmbd21.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hadoop implementation of CourseWork
 * @author maymon
 *
 */
public class AirportFlightsManager {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Set the starting class to run with this job.
		Job job = Job.getInstance(conf);
		job.setJarByClass(AirportFlightsManager.class);
		job.setJobName("AirportFlightsPassengerCount");

		// Set the key/value types for the overall job output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set the mapper and reducer classes to use
		job.setMapperClass(FlightsMapper.class);
		job.setReducerClass(FlightsReducer.class);

		// Set the input and output formats (tab-separated,
		// newline-delimited text files)
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		    
		// Set the locations in the DFS of the input and output files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Run the job and wait for it to finish
		job.waitForCompletion(true);
	}
	
	public static class FlightsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text passengerId = new Text();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	        String[] line = value.toString().split(",");
	        passengerId.set(line[0]);

	        context.write(passengerId, one);
	    }
	}
	
	public static class FlightsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        context.write(key, new IntWritable(sum));
	    }
	}
	
	
	
}
