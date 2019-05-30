import exceptions.DBException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class represents a relation in DavidDB.
 * @author David
 * @version 6/5/18
 */
public class Relation extends AbstractRelation {
	protected Map<String, AttributeMapEntry> attribute_map;

	/**
	 * Creates an empty relation without a name
	 */
	public Relation() {
		this(null);
	}

	/**
	 * Creates an empty relation with no attributes with the given name
	 * @param name the name of the relation; null if nameless
	 */
	public Relation(String name) {
		super(name);
		this.attribute_map = new HashMap<>();
	}

	/**
	 * Populates this relation with data from the given file.
	 * @param infile the name of the data file
	 * @throws FileNotFoundException if file does not exist
	 * @throws DBException if an attribute value does not match the attribute's type
	 */
	@Override
	public void read(String infile) throws FileNotFoundException, DBException {
		BufferedReader fin = new BufferedReader(new FileReader(infile));
		String line;
		try {
			// each line contains actual data
			while ((line = fin.readLine()) != null) {
				String[] attr_val = line.split("\\|");
				Comparable[] tuple_values = new Comparable[attr_val.length];

				// ensure each data value matches the attribute's type
				for (int i = 0; i < this.attribute_list.size(); i++) {
					Attribute.Type type = this.attribute_list.get(i).getType();
					switch (type) {
						case TEXT:
							if (attr_val[i].matches("^'.*'$")) {
								tuple_values[i] = attr_val[i];
							}
							else if (attr_val[i].equalsIgnoreCase("null")) {
								tuple_values[i] = null;
							}
							else {
								throw new DBException("Type mismatch for TEXT attribute: "
										+ attr_val[i] + " in " + infile);
							}
							break;
						case NUMERIC:
							try {
								tuple_values[i] = Double.parseDouble(attr_val[i]);
							} catch (NumberFormatException e) {
								throw new DBException("Type mismatch for NUMERIC attribute: "
										+ attr_val[i] + " in " + infile);
							}
							break;
						default:
							// code should not reach here
					}
				}
				// add the tuple to the set
				this.addTuple(new Tuple(tuple_values,this));
			}
			fin.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Assigns a list of attributes
	 * @param list a list of attributes
	 */
	@Override
	public void setAttributes(List<Attribute> list) {
		super.setAttributes(list);
		this.attribute_map.clear();
		for (int i = 0; i < attribute_list.size(); i++) {
			AttributeMapEntry entry = this.attribute_map.get(this.attribute_list.get(i).getName());
			if (entry == null) {
				entry = new AttributeMapEntry(i, 0);
			}
			entry.count++;

			AttributeMapEntry ped_entry = this.attribute_map.get(this.attribute_list.get(i).getPedanticName());
			if (ped_entry == null) {
				ped_entry = new AttributeMapEntry(i, 0);
			}
			ped_entry.count++;

			this.attribute_map.put(this.attribute_list.get(i).getName(), entry);
			this.attribute_map.put(this.attribute_list.get(i).getPedanticName(), ped_entry);
		}
	}

	/**
	 * Inserts the given tuple to the current relation.
	 * @param new_tuple the tuple to be added to the relation
	 */
	@Override
	public void addTuple(Tuple new_tuple) {
		if (new_tuple != null) {
			if (new_tuple.size() == this.attribute_list.size()) {
				this.tuples.add(new_tuple);
			}
			else {
				throw new IllegalArgumentException("Tuple size mismatch: " +
						new_tuple.size() + " but relation contains " +
						this.attribute_list.size() + " attributes.");
			}
		}
	}

	/**
	 * @return the string representation of the relation's definition
	 */
	@Override
	public String schemaToString() {
		StringBuilder sb = new StringBuilder();
		List<Attribute> list = this.getAttributes();
		sb.append(this.name).append("(\n");
		for (int i = 0; i < list.size(); i++) {
			sb.append("\t");
			sb.append(list.get(i).getName()).append(" ");
			sb.append(list.get(i).getType().toString());
			if (i < list.size()-1) {
				sb.append(", ");
			}
			sb.append("\n");
		}
		sb.append(")");
		return sb.toString();
	}

	/**
	 * (Hwk2): Now requires discernment of duplicate attribute names
	 * @return a string representation of the current relation
	 */
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		StringBuilder line = new StringBuilder();

		// build a line banner
		for (int i = 0; i < (this.attribute_list.size()*Tuple.COL_SPACING+this.attribute_list.size()+1); i++) {
			line.append("-");
		}
		line.append("\n");

		// build the column labels: |col1|col2|...
		// use pedantic column names or not? Need to check if there's a duplicate
		boolean pedantic = this.duplicateAttr();

		ret.append(Tuple.COL_SEPARATOR);
		for (int i = 0; i < this.attribute_list.size(); i++) {
			// append the column name
			String col_name = (pedantic) ? this.attribute_list.get(i).getPedanticName() :
					this.attribute_list.get(i).getName();
			if (col_name.length() > Tuple.COL_SPACING) {
				col_name = col_name.substring(0, Tuple.COL_SPACING);
			}
			ret.append(col_name);

			// deal with filler spaces
			int fill_len = Tuple.COL_SPACING - col_name.length();
			for (int fill = 0; fill < fill_len; fill++) {
				ret.append(" ");
			}
			if (i < this.attribute_list.size()-1) {
				ret.append(Tuple.COL_SEPARATOR);
			}
		}
		ret.append(Tuple.COL_SEPARATOR).append("\n");

		// insert streamer before and after the column names
		ret.insert(0, line);
		if (this.name != null) {
			ret.insert(0, this.name + "\n");
		}
		ret.append(line);

		// now put each tuple on a separate row
		if (this.tuples.isEmpty()) {
			ret.append("(Empty)\n");
		}
		else {
			for (Tuple t : this.tuples) {
				ret.append(t.toString()).append("\n");
			}
		}
		ret.append(line);				// insert streamer after the data
		return ret.toString();
	}

	/**
	 * (Hwk2 addition for toString())
	 * Determines if the list of attributes contains duplicate values
	 * @return true if duplicate if found, and false otherwise
	 */
	public boolean duplicateAttr() {
		for (int i = 0; i < this.attribute_list.size(); i++) {
			for (int j = i+1; j < this.attribute_list.size(); j++) {
				if (i != j && this.attribute_list.get(i).getName()
						.equalsIgnoreCase(this.attribute_list.get(j).getName())) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * (Hwk 3 addition)
	 * Looks up the list position of the given attribute
	 * @param attr_name	a (pedantic) name for the attribute
	 * @return	position of the attribute in the list, or -1 if it does not exist
	 * @throws DBException if attribute does not exist or is ambiguous
	 */
	public int lookup(String attr_name) {
		AttributeMapEntry entry = this.attribute_map.getOrDefault(attr_name, null);
		if (entry == null) {
			throw new DBException("Attribute: " + attr_name + " does not exist in relation "
					+ this.getName());
		}
		if (entry.count > 1) {
			throw new DBException("Attribute: " + attr_name + " is ambiguous in relation "
					+ this.getName());
		}
		return entry.getPos();
	}

	/**
	 * Inner class to provide fast location for attributes
	 */
	static class AttributeMapEntry {
		private int count;
		private int pos;

		/**
		 * Creates a map entry
		 * @param pos	position of the attribute in the list
		 * @param count the number of times this attribute appears
		 */
		public AttributeMapEntry(int pos, int count) {
			this.setPos(pos);
			this.count = count;
		}

		public int getPos() {
			return pos;
		}

		public void setPos(int pos) {
			this.pos = pos;
		}
		public int getCount() {
			return count;
		}
	}
}