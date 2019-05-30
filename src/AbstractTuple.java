import exceptions.DBException;
import java.util.List;

/**
 * NOTE: DO NOT MODIFY THIS CLASS
 *
 * This class models a tuple in the relational data model. A tuple is an
 * ordered list of values.
 *
 * @author David
 * @version 9/2/2018
 */
public abstract class AbstractTuple implements Cloneable {
	protected List<Comparable> data;
	protected AbstractRelation relation;

	/**
	 * @return a clone of the current tuple
	 */
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	/**
	 * Assigns the relation to which this tuple belongs.
	 * @param r	a reference to the Relation
	 */
	public void setRelation(AbstractRelation r) {
		this.relation = r;
	}

	/**
	 * @return number of values in the current tuple
	 */
	public int size() {
		return this.data.size();
	}

	/**
	 * Determines if two tuples are equal (all attribute values must match)
	 * @param other reference to another tuple
	 * @return true if both match, false otherwise
	 */
	@Override
	public abstract boolean equals(Object other);

	/**
	 * The tuple's string representation
	 * @return the tuple's string representation. Values are
	 * separated by COL_SEPARATOR
	 */
	@Override
	public abstract String toString();

	/**
	 * (Hwk 2 addition)
	 * @return a hashcode for this tuple
	 */
	@Override
	public abstract int hashCode();

	/**
	 * (Hwk 3 addition)
	 * Looks up and retrieves the value of the given attribute name.
	 * @param attr_name name of the attribute
	 * @return the value of the given attribute name
	 * @throws DBException if the attribute name does not exist
	 */
	public abstract Comparable valueOf(String attr_name) throws DBException;
}