package uk.ac.reading.compsci.csmbd21.cw;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reducer class responsible for distributed reducer phase.
 * @author Maymon
 *
 */
public class ReducerThread implements Callable<Map<Integer, List<String>>> {
	private static final Logger logger =  LoggerFactory.getLogger(ReducerThread.class);
	private Map<String, List<Integer>> shuffledResults = null;
	private String outputFilePrefix = null;

	public ReducerThread(Map<String, List<Integer>> shuffledResults, String outputFilePrefix) {
		this.shuffledResults = shuffledResults;
		this.outputFilePrefix = outputFilePrefix;
	}
	
	public String getOutputFilePrefix() {
		return outputFilePrefix;
	}

	/**
	 * Reads the chunk of data assigned to this reducer, and calculates the passenger with highest frequency.
	 * To mimick the hadoop environment, the reducer will produce an output chunk, prefixed with reducer number.
	 * 
	 * The Future returns mentioned here as return type are completed for the correctness only. For bigdata behaviour refer to output
	 * saved in the output files. The prefix of the output files can be configured in property file.
	 */
	public Map<Integer, List<String>> call() throws Exception {
		if (shuffledResults == null || shuffledResults.isEmpty()) {
			return null;
		}
		logger.info(outputFilePrefix+"--- starting   ReducerThread " );
		int highestFrequencyForThisReducer = 0;
		List<String> maxFrequencyPassengers = null;
		for (String key : shuffledResults.keySet()) {
			List<Integer> totalCountsForOnePassenger = shuffledResults.get(key);
			int totalCountOnePassenger = 0;
			for (Integer count : totalCountsForOnePassenger) {
				totalCountOnePassenger += count;
			}

			if (totalCountOnePassenger > highestFrequencyForThisReducer) {
				maxFrequencyPassengers = new ArrayList<String>();
				highestFrequencyForThisReducer = totalCountOnePassenger;
				// logger.info(key+"==="+highestFrequencyForThisReducer);
	
				maxFrequencyPassengers.add(key);
			}else if(totalCountOnePassenger == highestFrequencyForThisReducer) {
				maxFrequencyPassengers.add(key);
			}
		}
		try {
			FileWriter writer = new FileWriter(outputFilePrefix);
			writer.write(maxFrequencyPassengers + " : " + highestFrequencyForThisReducer);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info(outputFilePrefix+" "+maxFrequencyPassengers + " : " + highestFrequencyForThisReducer);
		
		Map<Integer, List<String>> result = new HashMap<Integer, List<String>>();
		result.put( new Integer(highestFrequencyForThisReducer), maxFrequencyPassengers);
		return result;
	}
}
