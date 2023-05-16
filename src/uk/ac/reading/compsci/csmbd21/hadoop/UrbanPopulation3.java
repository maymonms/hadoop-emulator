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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 2. Count the number of cities in each country larger than 100,000 people.
 * @author maymon
 *
 */
public class UrbanPopulation3 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Set the starting class to run with this job.
		Job job = Job.getInstance(conf);
		job.setJarByClass(UrbanPopulation.class);
		job.setJobName("CountofCitiesWithPopulationGreater");

		// Set the key/value types for the overall job output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set the mapper and reducer classes to use
		job.setMapperClass(UPMapper3.class);
		job.setReducerClass(UPReducer3.class);

		// Set the input and output formats (tab-separated,
		// newline-delimited text files)
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		    
		// Set the locations in the DFS of the input and output files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Run the job and wait for it to finish
		job.waitForCompletion(true);
	}
							
	public static class UPMapper3 extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text country = new Text();
        private final LongWritable cityPopulation = new LongWritable();
		
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String[] fields = value.toString().split("\t");
          if(fields[2].matches("^[\\w\\-]+$") && fields[4].matches("^\\d{1,9}$")) {
              country.set(fields[2]);
              cityPopulation.set(Long.parseLong(fields[4]));
              context.write(country, cityPopulation);
          }

        }
	}
	
	
	
	public static class UPReducer3  extends Reducer<Text, LongWritable, Text, LongWritable> {
									
		private final LongWritable count = new LongWritable();

		  @Override
		  protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		    long numCities = 0;
		    for (LongWritable value : values) {
		      if (value.get() > 100000) {
		        numCities++;
		      }
		    }
		    count.set(numCities);
		    context.write(key, count);
		  }
	}
	
	
}
