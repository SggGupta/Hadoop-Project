package com.edureka.project.airline.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.edureka.project.airline.common.AirLineIdKey;
import com.edureka.project.airline.common.AirlineCmdTypes;
import com.edureka.project.airline.common.AirlineDataRecord;
import com.edureka.project.airline.common.JoinGenericWritable;
import com.edureka.project.airline.common.RoutesDataRecord;
/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Reducer Class for handling Airline Data and Routes Data.
 *  
 *  
 */
public  class AirlineRoutesDataReducer extends Reducer<AirLineIdKey, JoinGenericWritable, NullWritable, Text>{
	private int airlineAppCmd = -1;
	Logger  log = Logger.getLogger(AirlineRoutesDataReducer.class);

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		
		if( config.get("airlineAppCmd") != null )
		{
			airlineAppCmd = Integer.parseInt(config.get("airlineAppCmd"));
		}
		log.debug(" AirlineRoutesDataReducer :Cmd type "+AirlineCmdTypes.getCommandString(airlineAppCmd));

	}
	@Override
	public void reduce(AirLineIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
        StringBuilder output = new StringBuilder();
        int noOfStop = -1;
        String sharedCode ="";
        boolean isZeroStopRouteFound = false;
        boolean isSharedCodeFound = false;
        boolean airlineRecordFound = false;
        int numberOfRoutes =0;
        for (JoinGenericWritable v : values) {
            Writable record = v.get();
            if (key.getRecordType().equals(AirLineIdKey.AIRLINE_RECORD)){
            		airlineRecordFound = true;
            	
            	AirlineDataRecord pRecord = (AirlineDataRecord)record;
                
            	output.append(Integer.parseInt(key.getAirlineID().toString())).append(", ");
                output.append(pRecord.getAirlineName().toString()).append(", ");
                output.append(pRecord.getCountry().toString());
        		log.debug(" AirlineRoutesDataReducer :AirLine Record "+output.toString());

            } else {
            	RoutesDataRecord record2 = (RoutesDataRecord)record;
            	noOfStop = Integer.parseInt(record2.getNumberOfStops().toString());
            	if(noOfStop == 0)
            	{
            		isZeroStopRouteFound = true;
            	}
            	sharedCode = record2.getCodeShare().toString();
            	if("Y".equals(sharedCode.trim()))
    			{
            		isSharedCodeFound = true;
    			}
            		numberOfRoutes = numberOfRoutes+1;
            		log.debug(" AirlineRoutesDataReducer :Routes Record "+output.toString());

            }
        }
        
        switch(airlineAppCmd)
        {
        	case AirlineCmdTypes.ZERO_STOP_CMD_VALUE:
        		if(airlineRecordFound && isZeroStopRouteFound )
    			{
        		            context.write(NullWritable.get(), new Text(output.toString() ));
    			    
    			}
        		break;
        	case AirlineCmdTypes.SHARED_CODE_CMD_VALUE:
        		if(airlineRecordFound && isSharedCodeFound)
    			{
		            context.write(NullWritable.get(), new Text(output.toString() ));

    			   
    			}
        		break;
        	case AirlineCmdTypes.ACTIVE_AIRLINE_CMD_VALUE:
        		if(airlineRecordFound && numberOfRoutes != 0)
    			{
		            context.write(NullWritable.get(), new Text(output.toString()+","+numberOfRoutes));
    			}
        		break;
        	
			    
	       
        }
       
    }
}
