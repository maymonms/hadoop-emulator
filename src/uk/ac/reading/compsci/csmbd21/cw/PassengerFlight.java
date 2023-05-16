package uk.ac.reading.compsci.csmbd21.cw;

public class PassengerFlight {
	private String passengerId;
	private String flightId;

	public PassengerFlight(String flightId, String passengerId) {
		this.flightId = flightId;
		this.passengerId = passengerId;
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof PassengerFlight)) {
			return false;
		}
		PassengerFlight other = (PassengerFlight) obj;
		return flightId.equals(other.flightId) && passengerId.equals(other.passengerId);
	}

	@Override
	public String toString() {
		return passengerId+" "+flightId;
	}

	
	@Override
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((flightId == null) ? 0 : flightId.hashCode());
	    result = prime * result + ((passengerId == null) ? 0 : passengerId.hashCode());
	    return result;
	}
}
