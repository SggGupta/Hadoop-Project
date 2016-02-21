package com.edureka.project.airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.edureka.project.airline.mapreduce.AirportTerritoryMapper;
import com.edureka.project.airline.mapreduce.AirportTerritoryReducer;

/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Program to find territory/Country having maximum airports.
 *  
 *  Usage: findTerritoryHavingMaxAirport <input-airports_mod>  <output> 
 */
public class FindAirportTerritory extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration config = getConf();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: findTerritoryHavingMaxAirport <input> <output>");
	     return -1;
	    }

		Job job = Job.getInstance(config);
		// Specify various job-specific parameters
		job.setJobName("Find territory having maximum airport");

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(AirportTerritoryMapper.class);
		job.setReducerClass(AirportTerritoryReducer.class);
	    //job.setCombinerClass(AirportTerritoryReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
				
		job.setJarByClass(FindAirportTerritory.class);

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FindAirportTerritory(), args);
		System.exit(exitCode);
	}

	

	
}
