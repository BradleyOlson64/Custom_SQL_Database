import exceptions.DBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * NOTE: DO NOT MODIFY THIS CLASS
 *
 * This class models a tuple in the relational data model. A tuple is an
 * ordered list of values.
 *
 * @author David
 * @version 5/26/2018
 */
public class Tuple extends AbstractTuple implements Cloneable {
	/** CONSTANTS */
	/* this symbol separates the values when showing tuple's string representation */
	public static final String COL_SEPARATOR = "|";

	/* the number of spaces to use for tuple's string representation */
	public static final int COL_SPACING = 16;

	/**
	 * Creates a tuple using the given array of values
	 * @param values
	 * @param r	relation to which this tuple belongs
	 */
	public Tuple(List<Comparable> values, AbstractRelation r) {
		this.data = values;
		this.relation = r;
	}

	/**
	 * Creates a tuple using the given array of values
	 * @param values
	 * @param r	relation to which this tuple belongs
	 */
    public Tuple(Comparable[] values, AbstractRelation r) {
		this.data = new ArrayList<>(Arrays.asList(values));
		this.relation = r;
    }

	/**
	 * Concatenates two tuples
	 * @param other a reference to another tuple
	 * @return the concatenation of the current tuple and the given tuple
	 */
	public Tuple concat(Tuple other) {
    	ArrayList<Comparable> tmp = new ArrayList<>();
    	tmp.addAll(this.data);
    	tmp.addAll(other.data);
		return new Tuple(tmp,null);
	}

	/**
	 * Determines if two tuples are equal (all attribute values must match)
	 * @param other reference to another tuple
	 * @return true if both match, false otherwise
	 */
	@Override
    public boolean equals(Object other) {
        Tuple other_tuple = (Tuple) other;
        if (this.data.size() != other_tuple.data.size()) {
        	return false;
		}
		for (int i = 0; i < this.data.size(); i++) {
			if (!this.data.get(i).equals(other_tuple.data.get(i))) {
				return false;
			}
		}
		return true;
    }

    /**
	 * The tuple's string representation
     * @return the tuple's string representation. Values are
     * separated by COL_SEPARATOR
     */
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(Tuple.COL_SEPARATOR);
        for (int i = 0; i < this.data.size(); i++) {
			String val = this.data.get(i).toString();
			if (val.length() > Tuple.COL_SPACING) {
				val = val.substring(0, Tuple.COL_SPACING);
			}
			str.append(val);

            // deal with spacing
			int fill_len = Tuple.COL_SPACING - this.data.get(i).toString().length();
			for (int fill = 0; fill < fill_len; fill++) {
				str.append(" ");
			}
            if (i < this.data.size()-1) {
                str.append(Tuple.COL_SEPARATOR);
            }
        }
        str.append(Tuple.COL_SEPARATOR);
        return str.toString();
    }

	/**
	 * (Hwk 2 addition)
	 * @return a hashcode for this tuple
	 */
	@Override
	public int hashCode() {
		int code = 0;
		for (Comparable val : this.data) {
			code += val.hashCode();
		}
		return code;
	}

	/**
	 * (Hwk 3 addition)
	 * Looks up and retrieves the value of the given attribute name.
	 * @param attr_name name of the attribute
	 * @return the value of the given attribute name
	 * @throws DBException if the attribute name does not exist
	 */
	public Comparable valueOf(String attr_name) throws DBException {
		if (this.relation == null) {
			throw new DBException("Tuple's relation is not set.");
		}
		return this.data.get(((Relation)this.relation).lookup(attr_name));
	}
}