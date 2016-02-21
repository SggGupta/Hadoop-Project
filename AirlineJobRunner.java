package com.edureka.project.airline.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.edureka.project.airline.ZeroStopAirlineFinder.JoinGroupingComparator;
import com.edureka.project.airline.ZeroStopAirlineFinder.JoinSortingComparator;
import com.edureka.project.airline.common.AirLineIdKey;
import com.edureka.project.airline.common.JoinGenericWritable;
/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Class for doing common operation for -
 *  1. Finding Active Airlines
 *  2. Finding Zero Stop Airlines
 *  3. Finding Airlines having shared code
 */

public class AirlineJobRunner {

	
	public int invokeService(Configuration config, String[] args, Class invokerClass) throws IOException, ClassNotFoundException, InterruptedException
	{
		
		Job job = Job.getInstance(config);
	    job.setJarByClass(invokerClass);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(AirLineIdKey.class);
	    job.setMapOutputValueClass(JoinGenericWritable.class);
	                               
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RoutesDataMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirlineDataMapper.class);
	                              
	    job.setReducerClass(AirlineRoutesDataReducer.class);
	                         
	    job.setSortComparatorClass(JoinSortingComparator.class);
	    job.setGroupingComparatorClass(JoinGroupingComparator.class);
	                               
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	                               
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    Path outputFilePath = new Path(args[2]);
	    FileSystem fs = FileSystem.newInstance(config);

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }  
		
	}
}
