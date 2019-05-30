package perf;

public interface Timeable {
	/**
	 * @return the elapsed time (in milliseconds) since last reset.
	 */
	double getElapsedTime();

	/**
	 * Resets the elapsed time to zero.
	 */
	void resetElapsedTime();
}
