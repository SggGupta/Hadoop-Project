package com.edureka.project.airline.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Mapper Class for Airport Finder .
 *  
 *  
 */
public  class AirportMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	 public String filterCountry = "";  
	  @Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String recordValues[] =value.toString().split(",");
      Text keyText = new Text(recordValues[0]);
      
      Text valueText = new Text(recordValues[1]+","+recordValues[2]+","+recordValues[3]);
      if(filterCountry.equals(recordValues[3]))
		{
			context.write(keyText, valueText);
		}
	}
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		
		if( config.get("country") != null )
		{
			filterCountry = config.get("country");

		}
	}
}
