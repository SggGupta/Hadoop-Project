package com.edureka.project.airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.edureka.project.airline.common.AirLineIdKey;
import com.edureka.project.airline.common.AirlineCmdTypes;
import com.edureka.project.airline.mapreduce.AirlineJobRunner;
/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Program to find airlines having Zero Stops. By default it will find  all airlines having
 *  Zero Stops. But this program also takes optional parameter if user want find airlines having
 *  Zero Stops in specific country.
 *  
 *  Usage: zeroStopAirlinesFinder <input-routesData> <input-airlines> <output> [country]
 */
public class ZeroStopAirlineFinder extends Configured implements Tool {

	
	public static void main(String[] args) throws Exception{                               
	   
	    int res = ToolRunner.run(new ZeroStopAirlineFinder(), args);
	    
	}
	
	@Override
	public int run(String[] allArgs) throws Exception {
		
		Configuration config = getConf();    
		String[] otherArgs = new GenericOptionsParser(config, allArgs).getRemainingArgs();
	    if (otherArgs.length < 3) {
	      System.err.println("Usage: zeroStopAirlinesFinder <input> <input> <out> [country]");
	     return -1;
	    }
		config.set("airlineAppCmd", AirlineCmdTypes.ZERO_STOP_CMD_VALUE+"");
	    // To filter out specific country

		if (otherArgs.length == 4)
		{
			String country = otherArgs[3];
			config.set("country", country);
		}
		AirlineJobRunner airlineJobRunner = new AirlineJobRunner();
		return airlineJobRunner.invokeService(config, otherArgs,ZeroStopAirlineFinder.class);
		
	}

	
	
	public static class JoinGroupingComparator extends WritableComparator {
	    public JoinGroupingComparator() {
	        super (AirLineIdKey.class, true);
	    }                             

	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	    	AirLineIdKey first = (AirLineIdKey) a;
	    	AirLineIdKey second = (AirLineIdKey) b;
	                      
	        return first.airlineID.compareTo(second.airlineID);
	    }
	}
	
	public static class JoinSortingComparator extends WritableComparator {
	    public JoinSortingComparator()
	    {
	        super (AirLineIdKey.class, true);
	    }
	                               
	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	    	AirLineIdKey first = (AirLineIdKey) a;
	    	AirLineIdKey second = (AirLineIdKey) b;
	                                 
	        return first.compareTo(second);
	    }
	}

}
