package solver;

import exceptions.DBException;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Expression {
	private final static ScriptEngine solver = (new ScriptEngineManager()).getEngineByName("JavaScript");

	/**
	 * Attempts to evaluate a boolean expression
	 * @param cond	A valid conditional expression
	 * @return	the result of the given expression
	 * @throws IllegalArgumentException if the given expression string is invalid
	 */
	public static boolean eval(String cond) throws DBException {
		// replace '=' with '=='
		cond = cond.replaceAll("\\s+=\\s+", " == ");
		try {
			return (Boolean) solver.eval(cond);
		} catch (ScriptException e) {
 			throw new DBException("Invalid expression: " + cond);
		}
	}
}
