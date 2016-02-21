package com.edureka.project;

import org.apache.hadoop.util.ProgramDriver;

import com.edureka.project.airline.ActiveAirlinesFinder;
import com.edureka.project.airline.AirlinesWithSharedCodeFinder;
import com.edureka.project.airline.FindAirport;
import com.edureka.project.airline.FindAirportTerritory;
import com.edureka.project.airline.ZeroStopAirlineFinder;
/**
 * 
 * @author Sandeep Ipte
 * @version 1.0
 * 
 *  Main Class use human-readable description to invoke appropriate class
 */
public class AirlineApp  {
	  
	  public static void main(String argv[]){
	    int exitCode = -1;
	    ProgramDriver programDriver = new ProgramDriver();
	    try {
	      programDriver.addClass("findAirportByCountry", FindAirport.class, 
	                   "A map/reduce program that counts the airport in the country.");
	      programDriver.addClass("findTerritoryHavingMaxAirport", FindAirportTerritory.class, 
                  "A map/reduce program that finds the territory/country having higheset number of airport.");
	      programDriver.addClass("zeroStopAirlinesFinder", ZeroStopAirlineFinder.class, 
                  "A map/reduce program that finds the airlines having zero stops routes.");
	      programDriver.addClass("airlinesWithSharedCodeFinder", AirlinesWithSharedCodeFinder.class, 
                  "A map/reduce program that finds the airlines having shared codes.");
	      programDriver.addClass("activeAirlinesFinder", ActiveAirlinesFinder.class, 
                  "A map/reduce program that finds the active airlines.");
	      
	      
	      exitCode = programDriver.run(argv);
	    }
	    catch(Throwable e){
	      e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	  }
	}
