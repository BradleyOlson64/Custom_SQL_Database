package exceptions;

/**
 * This class represents a checked database exception, to be thrown (and caught)
 * whenever a database processing error occurs.
 *
 * @author David
 * @version 5/23/2018
 */
public class DBException extends RuntimeException {
	public DBException() {
		super();
	}

	public DBException(String message) {
		super(message);
	}
}
