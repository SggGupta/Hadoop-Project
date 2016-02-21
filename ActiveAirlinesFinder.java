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
 *  Program to find active airlines in the Country. By default it will find active airlines in
 *  USA. But this program also takes optional parameter if user want find active airlines in 
 *  specific country.
 *  
 *  Usage: activeAirlinesFinder <input-routesData> <input-airlines> <output> [country]
 */
public class ActiveAirlinesFinder extends Configured implements Tool {

	
	public static void main(String[] args) throws Exception{                               
	   
	    int res = ToolRunner.run(new ActiveAirlinesFinder(), args);
	    
	}
	
	@Override
	public int run(String[] allArgs) throws Exception {
		Configuration config = getConf();    
		String country = "United States";
		String[] otherArgs = new GenericOptionsParser(config, allArgs).getRemainingArgs();
	    if (otherArgs.length < 3) {
	      System.err.println("Usage: activeAirlinesFinder <input> <input> <output> [country]");
	     return -1;
	    }
	    // To filter out specific country
		if (otherArgs.length == 4)
		{
			country = otherArgs[3];
		}
		config.set("airlineAppCmd", AirlineCmdTypes.ACTIVE_AIRLINE_CMD_VALUE+"");
		config.set("country", country);
		AirlineJobRunner airlineJobRunner = new AirlineJobRunner();
		return airlineJobRunner.invokeService(config, otherArgs,ActiveAirlinesFinder.class);
		
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
