package com.edureka.project.airline.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Mapper Class for Airport Territory Finder .
 *  
 *  
 */
public class AirportTerritoryMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	public static String filterCounter = "India";
	public static final IntWritable count = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String recordValues[] = value.toString().split(",");
		context.write(new Text(recordValues[3]), count);

	}
}
