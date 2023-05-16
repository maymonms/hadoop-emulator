package uk.ac.reading.compsci.csmbd21.cw;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper class responsible for distributed processing.
 * Process the data chunk assigned to it and produces a key-value map.
 * @author Maymon
 *
 */
public class MapperThread implements Callable<Map<String, Set<PassengerFlight>>> {
	private static final Logger logger =  LoggerFactory.getLogger(MapperThread.class);
	private String fileName;
	
    public MapperThread(String fileName) {
    	this.fileName = fileName;
    }
 
    /**
     * Actual thread logic for mapper goes here. Returns a set of PassengerFlight objects.
     * The equals function of PassengerFlight implemented in such a way that it remove the 
     * duplicates automatically.   
     */
    public Map<String, Set<PassengerFlight>> call() throws Exception {
    	logger.info("Starting MapperThread with input file "+fileName +"------>");
    	Map<String, Set<PassengerFlight>> outputMap = new HashMap<String, Set<PassengerFlight>>();
    	int count = 0;
    	try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            
            while ((line = br.readLine()) != null) {
            		count++;
                    // process the line
                    String[] columns = line.split(",");
                    String passengerId = columns[0];
                    String departureTime = columns[1];
                    
					if (outputMap.containsKey(passengerId)) {
						Set<PassengerFlight> passegerFlightSet = outputMap.get(passengerId);
						PassengerFlight pf = new PassengerFlight(passengerId, departureTime);
						passegerFlightSet.add(pf);
						outputMap.put(passengerId, passegerFlightSet);

					} else {
						Set<PassengerFlight> passegerFlightSet = new HashSet<PassengerFlight>();
						PassengerFlight pf = new PassengerFlight(passengerId, departureTime);
						passegerFlightSet.add(pf);
						outputMap.put(passengerId, passegerFlightSet);
					}
                    
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    	
    	
    	
      	logger.info(count+"----- Total rows processed in this thread "+outputMap.size());
    	return outputMap;
    }



    
    
}
