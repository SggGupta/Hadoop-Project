package com.edureka.project.airline.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.edureka.project.airline.common.AirLineIdKey;
import com.edureka.project.airline.common.AirlineCmdTypes;
import com.edureka.project.airline.common.JoinGenericWritable;
import com.edureka.project.airline.common.RoutesDataRecord;
/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Mapper Class for handling Routes Data .
 *  
 *  
 */
public  class RoutesDataMapper extends Mapper<LongWritable, Text, AirLineIdKey, JoinGenericWritable>{
    
	private int airlineAppCmd = -1;
	Logger log = Logger.getLogger(RoutesDataMapper.class);
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		
		if( config.get("airlineAppCmd") != null )
		{
			airlineAppCmd = Integer.parseInt(config.get("airlineAppCmd"));
		}
		log.debug(" RoutesDataMapper :Cmd type "+AirlineCmdTypes.getCommandString(airlineAppCmd));
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
        String[] recordFields = value.toString().split(",");
        
        
        try {
			int airlineId = Integer.parseInt(recordFields[1]);
			
			int noOfStop = Integer.parseInt(recordFields[7]);
			 String sharedCode = recordFields[6];
			 boolean processData = false;
			switch(airlineAppCmd)
	        {
	        	case AirlineCmdTypes.ZERO_STOP_CMD_VALUE:
	        		
	        		if(noOfStop == 0)
	    			{
	        			processData = true;
	    			     
	    			    
	    			}
	        		break;
	        	case AirlineCmdTypes.SHARED_CODE_CMD_VALUE:
	        		if("Y".equals(sharedCode))
	    			{
	        			processData = true;
	    			   
	    			}
	        		break;
	        	default :
	        		processData = true;
	        		
    			    break;
		       
	        }
			log.debug(" RoutesDataMapper :airline id "+airlineId+ " Stop "+noOfStop+ " Process "+processData);

			  if(processData)
			  {
				    AirLineIdKey recordKey = new AirLineIdKey(airlineId, AirLineIdKey.ROUTES_RECORD);
				    RoutesDataRecord record = new RoutesDataRecord(sharedCode, noOfStop);
				                                           
				    JoinGenericWritable genericRecord = new JoinGenericWritable(record);
				    context.write(recordKey, genericRecord);
			  }

			
		} catch (NumberFormatException e) {
			log.error(" RoutesDataMapper :Ignoring Number Format Exception");

		}
        
       
    }
}
