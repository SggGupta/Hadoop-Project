package com.edureka.project.airline.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Reducer Class for Airport Finder .
 *  
 *  
 */
public class AirportReducer extends Reducer<Text,IntWritable,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	Text strText = new Text();
    	for (Text val : values) {
    		strText.set(val);
    	 context.write(key, strText);
    	}
    }
}
