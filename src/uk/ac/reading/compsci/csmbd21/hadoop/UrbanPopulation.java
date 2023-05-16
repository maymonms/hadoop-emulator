package uk.ac.reading.compsci.csmbd21.hadoop;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * 
 * First task: Count the population per country
 * 
 * @author maymon
 *
 */
public class UrbanPopulation {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Set the starting class to run with this job.
		Job job = Job.getInstance(conf);
		job.setJarByClass(UrbanPopulation.class);
		job.setJobName("UrbanPopulation");

		// Set the key/value types for the overall job output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set the mapper and reducer classes to use
		job.setMapperClass(UPMapper.class);
		job.setReducerClass(UPReducer.class);

		// Set the input and output formats (tab-separated,
		// newline-delimited text files)
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the locations in the DFS of the input and output files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Run the job and wait for it to finish
		job.waitForCompletion(true);
	}

	// The mapper takes LongWritable key and Text value as input,
	// and produces Text key and IntWritable value as output.
	public static class UPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text country = new Text();
		private IntWritable population = new IntWritable();
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String cols[] = line.split("\t");
			
			// Data Validation using RegEx
			if(cols[2].matches("^[\\w\\-]+$") && cols[4].matches("^\\d{1,9}$")) {
				country.set(cols[2]);
				population.set(Integer.parseInt(cols[4]));
				context.write(country, population);
			}
		}
	}

	// The reducer takes Text key and IntWritable value,
	// and returns a Text key and IntWritable value
	public static class UPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for(IntWritable val : values)
				sum += val.get();
			
			context.write(key, new IntWritable(sum));
		}
	}

}