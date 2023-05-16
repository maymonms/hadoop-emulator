package uk.ac.reading.compsci.csmbd21.cw;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Read the configuration details
 * 
 * @author Maymon
 *
 */
public class ConfigurationProperties {
	public static Properties properties = new Properties();
	static {
        try (InputStream inputStream = new FileInputStream("resources\\config.properties")) {
        	properties.load(inputStream);
        } catch (IOException e) {
            System.err.println("Error loading properties file: " + e.getMessage());
        }
	}
}
