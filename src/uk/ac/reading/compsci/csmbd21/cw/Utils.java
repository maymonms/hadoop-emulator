package uk.ac.reading.compsci.csmbd21.cw;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashSet;
import java.util.Set;

public class Utils {

	/**
	 * This function retrieves the total number of lines in a file, without iterating each line.
	 * 
	 * @param filename
	 * @return
	 * @throws IOException
	 */
    public static int countLines(String filename) throws IOException {
        LineNumberReader reader = new LineNumberReader(new FileReader(filename));
        reader.skip(Long.MAX_VALUE); 
        int count = reader.getLineNumber() + 1; 
        reader.close();
        return count;
    }
    
    
	/**
	 * Get count of unique passengers. Assumes that first column in CSV is passenger detail.
	 * @param fileName
	 * @return
	 */
    public static int getTotalUniqueKeys(String fileName) {
		Set passengers = new HashSet<String>();
		try {
			String line = null;
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			while ((line = br.readLine()) != null) {
				String[] columns = line.split(",");
				String passengerId = columns[0];
				passengers.add(passengerId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return passengers.size();
	}

}
