package uk.ac.reading.compsci.csmbd21.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CityPopulation implements Writable {

	  private String city;
	  private long population;

	  public CityPopulation() {}

	  public CityPopulation(String city, long population) {
	    this.city = city;
	    this.population = population;
	  }

	  public void setCity(String city) {
	    this.city = city;
	  }

	  public void setPopulation(long population) {
	    this.population = population;
	  }

	  public String getCity() {
	    return city;
	  }

	  public long getPopulation() {
	    return population;
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeUTF(city);
	    out.writeLong(population);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    city = in.readUTF();
	    population = in.readLong();
	  }
	  
		  @Override
		public String toString() {			
			return city+" "+population;
		}
	}
