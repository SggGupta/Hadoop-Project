package com.edureka.project.airline.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.edureka.project.airline.common.AirLineIdKey;
import com.edureka.project.airline.common.AirlineCmdTypes;
import com.edureka.project.airline.common.AirlineDataRecord;
import com.edureka.project.airline.common.JoinGenericWritable;

/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Mapper Class for handling Airline Data .
 *  
 *  
 */
public  class AirlineDataMapper extends Mapper<LongWritable, Text, AirLineIdKey, JoinGenericWritable>{
   
	
	private int airlineAppCmd = -1;
	private String filterCountry = null;
	Logger  log = Logger.getLogger(AirlineDataMapper.class);
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		
		if( config.get("airlineAppCmd") != null )
		{
			airlineAppCmd = Integer.parseInt(config.get("airlineAppCmd"));
		}
		filterCountry = null;
		if( config.get("country") != null )
		{
			filterCountry = config.get("country");
		}
		log.debug(" AirlineDataMapper :Cmd type "+AirlineCmdTypes.getCommandString(airlineAppCmd)+ "Filter Country "+filterCountry);

	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",");
        int airlineId = Integer.parseInt(recordFields[0]);
        String airlineName = recordFields[1];
        String country = recordFields[6];
        String isActive = recordFields[7];
        
        boolean processRecord = false;
        switch(airlineAppCmd)
        {
        	case AirlineCmdTypes.ACTIVE_AIRLINE_CMD_VALUE:
	        		if(filterCountry.equalsIgnoreCase(country) && "Y".equals(isActive))   
	                {
	        			processRecord = true;
	                }
	        		
	        		break;
        		
        	default :
        		    if(filterCountry == null)
        		    {
        		    	processRecord = true;
        		    }
        		    else 
        		    {
        		    	if(filterCountry.equalsIgnoreCase(country) )   
    	                {
    	        			processRecord = true;
    	                }
    	        		
        		    }
        		    	
	    	        break;
        		
        }
        
         if(processRecord)
         {
        	 AirLineIdKey recordKey = new AirLineIdKey(airlineId, AirLineIdKey.AIRLINE_RECORD);
 	        AirlineDataRecord record = new AirlineDataRecord( airlineName,country,  isActive);
 	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
 	        context.write(recordKey, genericRecord);
         }
        
    }
}