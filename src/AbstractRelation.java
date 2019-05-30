import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

/**
 * This is an abstract class representing a relation in DavidDB.
 *
 * @author David
 * @version 6/3/2018
 */
public abstract class AbstractRelation {
	protected String name;
	protected List<Attribute> attribute_list;
	protected Set<Tuple> tuples;

	/**
	 * Creates an empty relation without a name
	 */
	public AbstractRelation() {
		this(null);
	}

	/**
	 * Creates an empty relation with no attributes with the given name
	 * @param name the name of the relation; null if nameless
	 */
	public AbstractRelation(String name) {
		this.name = name;
		this.tuples = new HashSet<>();
	}

	/**
	 * @return the name of the current relation
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Renames the current relation
	 * @param name	new name for the relation
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the list of attributes
	 */
	public List<Attribute> getAttributes() {
		return this.attribute_list;
	}

	/**
	 * Assigns a list of attributes
	 * @param list a list of attributes
	 */
	public void setAttributes(List<Attribute> list) {
		this.attribute_list = list;
	}

	/**
	 * @return the set of tuples currently stored
	 */
	public Set<Tuple> getTuples() {
		return this.tuples;
	}

	/**
	 * @return a deep copy of this relation
	 */
	public Object clone() {
		AbstractRelation r = null;
		try {
			r = new Relation();

			// deep copy attribute list
			List<Attribute> list = new ArrayList<>();
			for (Attribute a : this.attribute_list) {
				list.add(new Attribute(a.getRelation(), a.getType(), a.getName()));
			}
			r.setAttributes(list);	// also sets the new attribute-lookup map
			for (Tuple t : this.tuples) {
				Tuple new_tuple = (Tuple) t.clone();
				new_tuple.setRelation(r);
				r.addTuple(new_tuple);
			}
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return r;
	}

	/**
	 * Populates this relation with data from the given file.
	 * @param infile the name of the data file
	 * @throws FileNotFoundException if file does not exist
	 */
	public abstract void read(String infile) throws FileNotFoundException;

	/**
	 * Inserts the given tuple to the current relation.
	 * @param new_tuple the tuple to be added to the relation
	 */
	public abstract void addTuple(Tuple new_tuple);

	/**
	 * @return the string representation of the relation's definition
	 */
	public abstract String schemaToString();

	/**
	 * @return a string representation of the current relation
	 */
	public abstract String toString();
}