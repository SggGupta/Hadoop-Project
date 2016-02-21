package com.edureka.project.airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.edureka.project.airline.mapreduce.AirportMapper;
import com.edureka.project.airline.mapreduce.AirportReducer;
/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Program to find airport. By default it will find  all airport in India
 *  But this program also takes optional parameter if user want find airport 
 *  in specific country.
 *  
 *  Usage: findAirportByCountry <input-airports_mod>  <output> [country]
 */

public class FindAirport extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration config = getConf();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: findAirportByCountry <input> <output> [country]");
	     return -1;
	    }

		String country = "India";
		if (args.length == 3)
		{
			country = args[2];
		}
		
		config.set("country", country);
		Job job = Job.getInstance(config);
		// Specify various job-specific parameters
		job.setJobName("airport filter");

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(AirportMapper.class);
		job.setReducerClass(AirportReducer.class);
		job.setCombinerClass(AirportReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		

		job.setJarByClass(FindAirport.class);

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FindAirport(), args);
		System.exit(exitCode);
	}
	
	
	


}
