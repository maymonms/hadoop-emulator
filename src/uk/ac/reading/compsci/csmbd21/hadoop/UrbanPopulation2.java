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
 * @author maymon
 * 1. Report only the largest city in each country.
 * THis map-reduce finds the population of largest city for a country.
 */
public class UrbanPopulation2 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Set the starting class to run with this job.
		Job job = Job.getInstance(conf);
		job.setJarByClass(UrbanPopulation.class);
		job.setJobName("LargestCityInCountryByPopulation");

		// Set the key/value types for the overall job output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set the mapper and reducer classes to use
		job.setMapperClass(UPMapper2.class);
		job.setReducerClass(UPReducer2.class);

		// Set the input and output formats (tab-separated,
		// newline-delimited text files)
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CityPopulation.class);
		    
		// Set the locations in the DFS of the input and output files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Run the job and wait for it to finish
		job.waitForCompletion(true);
	}
	
	public static class UPMapper2 extends Mapper<LongWritable, Text, Text, CityPopulation> {
		private Text country = new Text();
        private final CityPopulation cityPopulation = new CityPopulation();

		
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String[] fields = value.toString().split("\t");
          if(fields[2].matches("^[\\w\\-]+$") && fields[4].matches("^\\d{1,9}$")) {
              country.set(fields[2]);
              cityPopulation.setCity(fields[1]);
              cityPopulation.setPopulation(Long.parseLong(fields[4]));
              context.write(country, cityPopulation);
          }

        }
	}
	
	
	
	public static class UPReducer2  extends Reducer<Text, CityPopulation, Text, CityPopulation> {
		
		private final CityPopulation largestCityPopulation = new CityPopulation();
		

		  @Override
		  protected void reduce(Text key, Iterable<CityPopulation> values, Context context) throws IOException, InterruptedException {
		    long maxPopulation = 0;
		    String largestCity = "";
		    for (CityPopulation value : values) {
		      if (value.getPopulation() > maxPopulation) {
		        maxPopulation = value.getPopulation();
		        largestCity = value.getCity();
		      }
		    }
		    largestCityPopulation.setCity(largestCity);
		    largestCityPopulation.setPopulation(maxPopulation);
		    context.write(key, largestCityPopulation);
		  }
	}
	
	
	

}
