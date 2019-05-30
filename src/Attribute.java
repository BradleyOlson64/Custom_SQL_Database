/**
 * NOTE: DO NOT MODIFY THIS CLASS
 *
 * This class defines the Attribute class and an enum that
 * lists all supported attribute data types for DavidDB.
 *
 * @author David
 * @version 5/29/2018
 */
public class Attribute implements Cloneable {
	/**
	 * Data type options
	 */
	public enum Type {
		NUMERIC, TEXT
	}

	/* fields */
	private AbstractRelation relation;	/* relation to which it belongs */
	private String name;		/* name of the attribute */
	private Type type;			/* data type */

	/**
	 * Creates a new attribute with the given relation and name
	 * @param name
	 */
	public Attribute(AbstractRelation relation, Type type, String name) {
		this.relation = relation;
		this.type = type;
		this.name = name;
	}

	/**
	 * @return the relation.name of this attribute, if relation has a name
	 */
	public String getPedanticName() {
		if (this.relation == null || this.relation.getName() == null ||
				this.relation.getName().equals("")) {
			return this.getName();
		}
		return this.relation.getName() + "." + this.getName();
	}

	/**
	 * @return the data type of this attribute
	 */
	public Type getType() {
		return this.type;
	}

	/**
	 * @return the relation to which the attribute belongs
	 */
	public AbstractRelation getRelation() {
		return this.relation;
	}

	/**
	 * Associates a new relation with this attribute
	 * @param r	a reference to a relation
	 */
	public void setRelation(AbstractRelation r) {
		this.relation = r;
	}

	/**
	 * Renames the attribute
	 * @param name	new name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the name of this attribute
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Two attributes are equal if their names are equal (ignoring case)
	 * @param other reference to another attribute
	 * @return true if equal, false otherwise
	 */
	@Override
	public boolean equals(Object other) {
		Attribute other_attr = (Attribute) other;
		return this.type.equals(other_attr.getType()) &&
				this.name.equalsIgnoreCase(other_attr.getName());
	}

	/**
	 * @return a hash code for this attribute
	 */
	@Override
	public int hashCode() {
		int code = 7;
		code += this.type.hashCode() * 13;
		code += this.name.hashCode() * 17;
		return code;
	}

	/**
	 * @return the atttribute's pedantic name
	 */
	@Override
	public String toString() {
		return this.getPedanticName();
	}
}