package com.edureka.project.airline.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Reducer Class for Airport Territory Finder .
 *  
 *  
 */
public  class AirportTerritoryReducer extends
Reducer<Text, IntWritable, Text, IntWritable> {

HashMap<String, Integer> countryCountMap = new HashMap<String, Integer>();

public void reduce(Text key, Iterable<IntWritable> values,
	Context context) throws IOException, InterruptedException {

int counter = 0;
for (IntWritable val : values) {
	counter = counter + val.get();

}
countryCountMap.put(key.toString(), counter);
}

@Override
protected void cleanup(Context context) throws IOException,
	InterruptedException {
String country = "Unknown";
int counter = 0;
Set<String> counterySet = countryCountMap.keySet();
Iterator<String> countryIterator = counterySet.iterator();
while (countryIterator.hasNext()) {
	String currentCountry = countryIterator.next();
	int currentCounter = countryCountMap.get(currentCountry);

	if (currentCounter > counter) {
		country = currentCountry;
		counter = currentCounter;
	}
}

context.write(new Text(country), new IntWritable(counter));

}
}
