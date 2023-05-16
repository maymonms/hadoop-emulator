package uk.ac.reading.compsci.csmbd21.cw;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Maymon
 *
 */
public class HadoopEmulator {
	private static final Logger logger = LoggerFactory.getLogger(HadoopEmulator.class);
	private static final String NUM_REDUCERS = "number_of_reducers";
	private static final String PREFIX_FOR_INPUT_SPLIT = "prefix_for_input_split";
	private static final String PREFIX_FOR_OUTPUT_FOLDERS = "output_folder_prefix";
	private static final String FILE_NAME = "file_name";
	private static final String MAX_ROWS_PER_MAPPER = "max_rows_per_node";

	public static void main(String[] args) throws Exception {
		ExecutorService reducerThreadPool = null;
		ExecutorService mapperThreadPool = null;
		logger.info("---- Hadoop Emulator  ------");
		Integer numReducers = Integer.parseInt(ConfigurationProperties.properties.getProperty(NUM_REDUCERS));
		String fileName = ConfigurationProperties.properties.getProperty(FILE_NAME);
		String prefixForInputSplit = ConfigurationProperties.properties.getProperty(PREFIX_FOR_INPUT_SPLIT);
		Integer maxRowsPerMapper = Integer
				.parseInt(ConfigurationProperties.properties.getProperty(MAX_ROWS_PER_MAPPER));

		try {
			List<MapperThread> mappers = inputSplittingIntoNodes(fileName, prefixForInputSplit, maxRowsPerMapper);
			logger.info("created mappers " + mappers.size());

			mapperThreadPool = Executors.newCachedThreadPool();
			List<Future<Map<String, Set<PassengerFlight>>>> mapperResults =  mapperThreadPool.invokeAll(mappers);

			logger.info("mapper phase finished");

			logger.info("--------  shuffler phase : starts  ");
			Map<String, Set<PassengerFlight>> shuffledResultsCombined = new HashMap<>();			

			//Combining mapper results from the mapper threads
			while (!mapperResults.isEmpty()) {
			    Iterator<Future<Map<String, Set<PassengerFlight>>>> it = mapperResults.iterator();
			    while (it.hasNext()) {
			        Future<Map<String, Set<PassengerFlight>>> future = it.next();
			        if (future.isDone()) {
			            try {
			                Map<String, Set<PassengerFlight>> resultMap = future.get();
			                logger.info("--- Getting future objects from one mapper thread "+resultMap.size());
							if (resultMap != null) {
								for (String key : resultMap.keySet()) {
									//logger.info("---key="+key);
									if (shuffledResultsCombined.containsKey(key)) {
										Set<PassengerFlight> set = shuffledResultsCombined.get(key);
										set.addAll(resultMap.get(key));
										shuffledResultsCombined.put(key, set);
									} else {
										Set<PassengerFlight> set = new HashSet<PassengerFlight>();
										set.addAll(resultMap.get(key));
										shuffledResultsCombined.put(key, set);
									}
								}
							}	
			            } catch (InterruptedException | ExecutionException e) {
			            	e.printStackTrace();
			            } finally {
			                // Remove the completed task from the list
			                it.remove();
			            }
			        } else {
			        	/**
			        	 * This block is for fault tolerance.  If a thread is not completed gracefully, it is 
			        	 * retried once.
			        	 */
			            // The task is still running, check if it has exceeded the timeout
			            long timeoutMillis = 1000; // Set the timeout in milliseconds
			            long startTimeMillis = System.currentTimeMillis();
			            while (!future.isDone() && (System.currentTimeMillis() - startTimeMillis) < timeoutMillis) {
			                Thread.sleep(100); // Wait for 100 milliseconds
			            }
			            if (!future.isDone()) {
			                // The task has exceeded the timeout, cancel it and resubmit
			                future.cancel(true);
			                Future<Map<String, Set<PassengerFlight>>> newFuture = mapperThreadPool.submit(mappers.get(0));
			                mapperResults.add(newFuture);
			            }
			        }
			    }
			}

			//shuffling the mapper results
			Map<String, List<Integer>> shuffledResults = new HashMap<>();
			for (String key : shuffledResultsCombined.keySet()) {

				if (shuffledResults.containsKey(key)) {
					List<Integer> list = shuffledResults.get(key);
					list.add(shuffledResultsCombined.get(key).size());

				} else {
					List<Integer> list = new ArrayList<Integer>();
					list.add(shuffledResultsCombined.get(key).size());
					shuffledResults.put(key, list);
				}

			}

			logger.info(""+shuffledResults);
			logger.info("number of items after schuffled state " + shuffledResults.size());
			//logger.info(""+Utils.getTotalUniqueKeys(fileName));
			reducerThreadPool = Executors.newCachedThreadPool();
			int partitionSizeForReducer = (int) Math.ceil(shuffledResults.size() / (double) numReducers);
			logger.info("=== partitionSizeForReducer=" + partitionSizeForReducer);

		
			//preparing reducers
			List<ReducerThread> reducers = new ArrayList<>();
			int reducerCount = 1;
			Map<String, List<Integer>> inputMapForReducer = new HashMap<>();
			reducers.add(new ReducerThread(inputMapForReducer,
					ConfigurationProperties.properties.getProperty(PREFIX_FOR_OUTPUT_FOLDERS) + reducerCount));
			Iterator iter = shuffledResults.entrySet().iterator();
			while (iter.hasNext()) {

				if (partitionSizeForReducer <= inputMapForReducer.size()) {
					logger.info("Allocating a new reducer thread, not started.");
					inputMapForReducer = new HashMap<>();
					reducerCount++;
					reducers.add(new ReducerThread(inputMapForReducer,
							ConfigurationProperties.properties.getProperty(PREFIX_FOR_OUTPUT_FOLDERS) + reducerCount));
				}
				for (int j = 0; j < partitionSizeForReducer; j++) {

					if (!iter.hasNext()) {
						break;
					}

					Map.Entry entry = (Map.Entry) iter.next();
					String key = (String) entry.getKey();
					inputMapForReducer.put(key, (List<Integer>) entry.getValue());
					// logger.info(j + " shuffling " + inputMapForReducer);
					iter.remove();
				}
			}
			logger.info("This needs to be empty:  " + shuffledResults.size());
			logger.info("--------  shuffler phase : end  --");

			
			logger.info("---- starting reducer phase  --");
			List<Future<Map<Integer, List<String>>>> reducerResultsinFuture = reducerThreadPool.invokeAll(reducers);
			
			/*
			 * The result is already saved into disk by corresponding threads. 
			 */
			Set<String> passengerIdSet = new HashSet<String>();
			int highestFrequency = 0;
			for(Future<Map<Integer, List<String>>> future: reducerResultsinFuture) {
				Map<Integer, List<String>> result = future.get(); 
				logger.info("joining future objects from reducer threads: "+result); 
				//There is only one element in this map.
				for(Integer frequency : result.keySet()) {
					if(frequency >= highestFrequency) {
						highestFrequency = frequency;
						passengerIdSet.addAll(result.get(frequency));
					}
				}
			}
			
			 
			logger.info("reducer phase finished");
			logger.info("The passenger details who travelled most are :"+passengerIdSet);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// Shut down the executor
			mapperThreadPool.shutdown();
			reducerThreadPool.shutdown();
		}

	}

	/**
	 * The input file is divided into to a number of input splits. In hadoop, these
	 * may be present in different nodes.
	 * 
	 * @param fileName            - Input file name.
	 * @param prefix              - Prefix to be added for the input splits.
	 * @param numberOfRowsForNode - Maximum number of rows that can be contained in
	 *                            one input split
	 * 
	 * @return Returns list mappers
	 */
	private static List<MapperThread> inputSplittingIntoNodes(String fileName, String prefix, int numberOfRowsForNode) {
		int numberOfNodes = 0;
		List<MapperThread> mappers = new ArrayList<MapperThread>();
		try {
			String line = null;
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			int counter = 0;

			String outFileName = prefix + "_" + numberOfNodes;
			FileWriter writer = new FileWriter(outFileName);
			while ((line = br.readLine()) != null) {
				if (counter >= numberOfRowsForNode) {
					writer.close();
					counter = 0;
					numberOfNodes++;
					mappers.add(new MapperThread(outFileName));
					outFileName = prefix + "_" + numberOfNodes;
					writer = new FileWriter(outFileName);
					writer.write(line + "\n");
				} else {
					writer.write(line + "\n");
				}
				counter++;
			}

			if (writer != null) {
				mappers.add(new MapperThread(outFileName));
				writer.flush();
				writer.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mappers;
	}

}
